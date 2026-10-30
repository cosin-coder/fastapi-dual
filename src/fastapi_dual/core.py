import asyncio
import json
import time
import httpx
import socket
import atexit
from typing import Callable, Awaitable, Optional, Dict
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from zeroconf import Zeroconf, ServiceBrowser, ServiceListener, InterfaceChoice

# ==========================================
# 🌟 全局实例与背压配置
# ==========================================
app = FastAPI(title="Dual Gateway", description="Built-in IoT Gateway with mDNS & Audit")
_semaphore = asyncio.Semaphore(5)
# 注意：超时时间设为 10 秒，兜底首次查询耗时
_client = httpx.AsyncClient(timeout=10.0) 

DualCallback = Callable[[dict], Awaitable[None]]

# ==========================================
# 🌟 终极 mDNS 模块：强制指定物理网卡
# ==========================================
_mdns_cache: Dict[str, dict] = {}
CACHE_TTL = 3600

# 获取本机真实内网 IP，用于锁定网卡
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 不会真正发送数据，但能获取当前路由使用的 IP
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

_LOCAL_IP = get_local_ip()
print(f"🌐 [mDNS] Binding to network interface: {_LOCAL_IP}")

# 🌟 核心修复：强制 Zeroconf 只使用主网卡，避免被 VMware/虚拟网卡吞掉 UDP 包
try:
    _zc = Zeroconf(interfaces=[_LOCAL_IP])
except Exception as e:
    print(f"⚠️ Failed to bind specific interface, falling back to default: {e}")
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
    # 尝试 3 次，每次等 2 秒（总耗时 6 秒），给路由器充足的时间转发组播包
    for _ in range(3):
        try:
            info = _zc.get_service_info("_http._tcp.local.", service_name, timeout=2000)
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

    cached = _mdns_cache.get(hostname)
    if cached and (current_time - cached["time"] < CACHE_TTL):
        return client_url.replace(hostname, cached["ip"]), True, cached["ip"]

    # 缓存没命中，强行发查询
    print(f"🔎 [mDNS] First-time lookup for {hostname}, please wait...")
    resolved_ip = await asyncio.to_thread(_sync_mdns_resolve, hostname)
    
    if resolved_ip:
        _mdns_cache[hostname] = {"ip": resolved_ip, "time": time.time()}
        print(f"✅ [mDNS Query] Resolved: {hostname} -> {resolved_ip}")
        return client_url.replace(hostname, resolved_ip), True, resolved_ip
    else:
        print(f"❌ [mDNS] Lookup failed for {hostname}. Is ESP32 online?")
        raise ConnectionError(f"mDNS resolution failed for {hostname}. Check your WiFi router isolation settings or firewall.")


# ==========================================
# 🌟 核心路由装饰器与业务逻辑
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