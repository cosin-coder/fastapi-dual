import asyncio
import json
import time
import httpx
import socket
import atexit
import inspect
from typing import Callable, Awaitable, Optional, Dict, List
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from zeroconf import Zeroconf, ServiceBrowser, ServiceListener
from pydantic import BaseModel

# ==========================================
# 🌟 全局实例与背压配置
# ==========================================
app = FastAPI(title="Dual Gateway", description="Built-in IoT Gateway with mDNS & Audit")
_semaphore = asyncio.Semaphore(5)
_client = httpx.AsyncClient(timeout=10.0) 

DualCallback = Callable[[dict], Awaitable[None]]

# ==========================================
# 🌟 动态预热注册表
# ==========================================
_warmup_devices: List[str] = []

def register_warmup(url: str):
    """供外部调用，注册需要启动预热的硬件地址"""
    if url not in _warmup_devices:
        _warmup_devices.append(url)

# ==========================================
# 🌟 mDNS 模块：全量网卡监听 + 启动预热
# ==========================================
_mdns_cache: Dict[str, dict] = {}
CACHE_TTL = 3600

print("🌐 [mDNS] Initializing Zeroconf on all interfaces...")
_zc = Zeroconf()

class MDNSListener(ServiceListener):
    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info and info.parsed_addresses():
            ip = info.parsed_addresses()[0]
            hostname = name.split("._http._tcp.local.")[0] + ".local"
            _mdns_cache[hostname] = {"ip": ip, "time": time.time()}
            print(f"📡 [mDNS Browser] Discovered: {hostname} -> {ip}")

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        hostname = name.split("._http._tcp.local.")[0] + ".local"
        if hostname in _mdns_cache:
            del _mdns_cache[hostname]
            print(f"🔌 [mDNS Browser] Removed: {hostname}")

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self.add_service(zc, type_, name)

_mdns_browser = ServiceBrowser(_zc, "_http._tcp.local.", MDNSListener())
atexit.register(lambda: _zc.close())

def _sync_mdns_resolve(hostname: str) -> Optional[str]:
    service_name = f"{hostname.replace('.local', '')}._http._tcp.local."
    print(f"🔎 [mDNS] Active querying for {service_name}...")
    
    for i in range(3):
        try:
            info = _zc.get_service_info("_http._tcp.local.", service_name, timeout=2000)
            if info and info.parsed_addresses():
                return info.parsed_addresses()[0]
        except Exception as e:
            print(f"⚠️ [mDNS] Query attempt {i+1} failed: {e}")
        time.sleep(0.5)
    return None

async def _resolve_client_url(client_url: str) -> tuple[str, bool, str]:
    if ".local" not in client_url:
        return client_url, False, ""

    hostname = client_url.split("//")[-1].split("/")[0].split(":")[0]
    current_time = time.time()

    cached = _mdns_cache.get(hostname)
    if cached and (current_time - cached["time"] < CACHE_TTL):
        return client_url.replace(hostname, cached["ip"]), True, cached["ip"]

    resolved_ip = await asyncio.to_thread(_sync_mdns_resolve, hostname)
    
    if resolved_ip:
        _mdns_cache[hostname] = {"ip": resolved_ip, "time": time.time()}
        print(f"✅ [mDNS Query] Resolved: {hostname} -> {resolved_ip}")
        return client_url.replace(hostname, resolved_ip), True, resolved_ip
    else:
        print(f"❌ [mDNS] Lookup failed for {hostname}.")
        raise ConnectionError(
            f"mDNS resolution failed for {hostname}. 请检查："
            f"1. ESP32 是否开机且连上同一 WiFi？"
            f"2. Windows 防火墙是否拦截了 Python 的 UDP 入站？"
            f"3. 路由器是否开启了 AP 隔离（访客模式）？"
        )

# ==========================================
# 🌟 启动预热机制
# ==========================================
@app.on_event("startup")
async def startup_warmup():
    async def warmup_task():
        await asyncio.sleep(1)
        for url in _warmup_devices:
            try:
                print(f"🔥 [Warmup] Pre-resolving {url}...")
                await _resolve_client_url(url)
            except Exception as e:
                print(f"⚠️ [Warmup] Failed to pre-resolve {url}: {e}")
                
    asyncio.create_task(warmup_task())

# ==========================================
# 🌟 核心装饰器 1：普通本地路由（不走硬件）
# ==========================================
def local_route(path: str, method: str = "GET"):
    """
    普通本地路由装饰器。
    不请求外部硬件，完全由 FastAPI 和本地 Python 代码处理。
    """
    def decorator(func):
        app.add_api_route(path, func, methods=[method])
        return func
    return decorator

# ==========================================
# 🌟 核心装饰器 2：双面硬件路由（走底层网络与 mDNS）
# ==========================================
def route(path: str, method: str = "GET", client_url: str = "", cb: Optional[DualCallback] = None):
    def decorator(func):
        endpoint = _create_endpoint(func, path, method, client_url, cb)
        app.add_api_route(path, endpoint, methods=[method])
        return func
    return decorator

def _create_endpoint(func, path, method, client_url, cb):
    # 通过反射获取函数的类型注解
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    target_model = None
    
    if params:
        annotation = params[0].annotation
        # 检查第一个参数是否标注了 Pydantic 模型
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            target_model = annotation

    async def endpoint(request: Request):
        start_time = time.perf_counter()

        ctx = {
            "path": path, "method": method, "original_client_url": client_url,
            "resolved_client_url": client_url, "mdns_resolved": False, "resolved_ip": "",
            "client_ip": request.client.host if request.client else "unknown",
            "query_params": dict(request.query_params), "esp_status": 0,
            "esp_data": None, "result": None, "error": None, "elapsed_ms": 0
        }

        if _semaphore.locked():
            ctx["error"] = "ESP32_BUSY"
            if cb: await _safe_call_cb(cb, ctx)
            return JSONResponse(status_code=429, content={"error": "Too many requests"})

        async with _semaphore:
            try:
                resolved_url, is_mdns, ip = await _resolve_client_url(client_url)
                ctx["resolved_client_url"] = resolved_url
                ctx["mdns_resolved"] = is_mdns
                ctx["resolved_ip"] = ip
            except ConnectionError as e:
                ctx["error"] = str(e)
                ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                if cb: await _safe_call_cb(cb, ctx)
                return JSONResponse(status_code=502, content={"error": str(e)})

            full_url = f"{resolved_url.rstrip('/')}{path}"
            if request.query_params:
                full_url += f"?{request.url.query}"

            req_body = None
            req_headers = {}
            if method.upper() in ["POST", "PUT", "PATCH"]:
                req_body = await request.body()
                if "content-type" in request.headers:
                    req_headers["Content-Type"] = request.headers["content-type"]

            try:
                resp = await _client.request(method=method.upper(), url=full_url, content=req_body, headers=req_headers)
                ctx["esp_status"] = resp.status_code
                raw_text = resp.text
            except Exception as e:
                ctx["error"] = str(e)
                raw_text = f'{{"error": "Failed to connect to hardware: {str(e)}"}}'

            # 智能解析硬件返回
            parsed_dict = None
            try:
                parsed_dict = json.loads(raw_text)
            except json.JSONDecodeError:
                pass

            if target_model is not None:
                # 如果底层返回了 error（比如硬件离线）
                if isinstance(parsed_dict, dict) and "error" in parsed_dict:
                    ctx["error"] = parsed_dict["error"]
                    ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                    if cb: await _safe_call_cb(cb, ctx)
                    return JSONResponse(status_code=502, content={"error": ctx["error"]})
                
                # 自动反序列化为 Pydantic 对象
                try:
                    if isinstance(parsed_dict, dict):
                        esp_data = target_model(**parsed_dict)
                    else:
                        esp_data = target_model()
                except Exception as e:
                    ctx["error"] = f"Pydantic validation failed: {e}"
                    ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                    if cb: await _safe_call_cb(cb, ctx)
                    return JSONResponse(status_code=500, content={"error": ctx["error"]})
            else:
                # 否则保持原来的 dict / str 格式
                esp_data = parsed_dict if parsed_dict is not None else raw_text
                
            ctx["esp_data"] = esp_data

            try:
                # 将解析好的对象直接传给业务函数
                result = await func(esp_data)
                ctx["result"] = result
            except Exception as e:
                ctx["error"] = f"Local handler error: {str(e)}"
                if cb:
                    ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                    await _safe_call_cb(cb, ctx)
                return JSONResponse(status_code=500, content={"error": ctx["error"]})

            ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
            if cb: await _safe_call_cb(cb, ctx)

            if isinstance(result, str):
                if result.strip().startswith("<"):
                    return Response(content=result, media_type="text/html")
                return Response(content=result, media_type="text/plain")

            return result
    return endpoint

async def _safe_call_cb(cb, ctx):
    try:
        await cb(ctx)
    except Exception as e:
        print(f"⚠️ Callback execution failed: {e}")