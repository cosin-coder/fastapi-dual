import asyncio
import json
import time
import httpx
import atexit
from typing import Callable, Awaitable, Optional, Dict
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from zeroconf import Zeroconf, ServiceBrowser, ServiceListener

app = FastAPI(title="Dual Gateway", description="Built-in IoT Gateway with mDNS & Audit")
_semaphore = asyncio.Semaphore(5)
_client = httpx.AsyncClient(timeout=10.0)

DualCallback = Callable[[dict], Awaitable[None]]

# ==========================================
# 🌟 终极 mDNS 模块：后台监听 + 主动查询兜底
# ==========================================
_mdns_cache: Dict[str, dict] = {}
CACHE_TTL = 3600

# 1. 创建全局唯一的 Zeroconf 实例（复用连接，避免端口冲突）
_zc = Zeroconf()

# 2. 定义后台监听器（自动发现局域网内的设备）
class MDNSListener(ServiceListener):
    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        """当发现新设备时触发"""
        info = zc.get_service_info(type_, name)
        if info and info.parsed_addresses():
            ip = info.parsed_addresses()[0]
            # 从 name 提取 hostname (例如 tb-2300)
            hostname = name.split("._http._tcp.local.")[0] + ".local"
            _mdns_cache[hostname] = {"ip": ip, "time": time.time()}
            print(f"📡 [mDNS Browser] Discovered & Cached: {hostname} -> {ip}")

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        """当设备离线时触发"""
        hostname = name.split("._http._tcp.local.")[0] + ".local"
        if hostname in _mdns_cache:
            del _mdns_cache[hostname]
        print(f"🔌 [mDNS Browser] Service removed: {hostname}")

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self.add_service(zc, type_, name)

# 3. 启动后台监听（它会自动接收局域网内的广播）
_mdns_browser = ServiceBrowser(_zc, "_http._tcp.local.", MDNSListener())

# 服务关闭时清理资源
atexit.register(lambda: _zc.close())

# 4. 主动查询兜底（如果后台没监听到，用户发请求时立刻发查询）
def _sync_mdns_resolve(hostname: str) -> Optional[str]:
    service_name = f"{hostname.replace('.local', '')}._http._tcp.local."
    # 连续发 3 次查询，每次等 1 秒，极大提升丢包环境下的成功率
    for _ in range(3):
        try:
            info = _zc.get_service_info("_http._tcp.local.", service_name, timeout=1000)
            if info and info.parsed_addresses():
                return info.parsed_addresses()[0]
        except Exception:
            pass
        time.sleep(0.2)
    return None

async def _resolve_client_url(client_url: str) -> tuple[str, bool, str]:
    if ".local" not in client_url:
        return client_url, False, ""

    hostname = client_url.split("//")[-1].split("/")[0].split(":")[0]
    current_time = time.time()

    # 优先从后台监听器的缓存中获取
    cached = _mdns_cache.get(hostname)
    if cached and (current_time - cached["time"] < CACHE_TTL):
        return client_url.replace(hostname, cached["ip"]), True, cached["ip"]

    # 如果缓存没有，主动发查询兜底
    resolved_ip = await asyncio.to_thread(_sync_mdns_resolve, hostname)
    
    if resolved_ip:
        _mdns_cache[hostname] = {"ip": resolved_ip, "time": time.time()}
        print(f"✅ [mDNS Query] Resolved & Cached: {hostname} -> {resolved_ip}")
        return client_url.replace(hostname, resolved_ip), True, resolved_ip
    else:
        print(f"❌ [mDNS] Resolution Failed: {hostname}")
        raise ConnectionError(f"mDNS resolution failed for {hostname}. Ensure the device is online.")


# ==========================================
# 🌟 核心路由装饰器 (不变)
# ==========================================
def route(path: str, method: str = "GET", client_url: str = "", cb: Optional[DualCallback] = None):
    def decorator(func):
        endpoint = _create_endpoint(func, path, method, client_url, cb)
        app.add_api_route(path, endpoint, methods=[method])
        return func
    return decorator

def _create_endpoint(func, path, method, client_url, cb):
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
            ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
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

            esp_data = raw_text
            try:
                esp_data = json.loads(raw_text)
            except json.JSONDecodeError:
                pass
            ctx["esp_data"] = esp_data

            try:
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