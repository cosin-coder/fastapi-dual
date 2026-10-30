import asyncio
import json
import time
import httpx
import threading
from typing import Callable, Awaitable, Optional, Dict
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from zeroconf import Zeroconf

# ==========================================
# 🌟 全局实例与背压配置
# ==========================================
app = FastAPI(title="Dual Gateway", description="Built-in IoT Gateway with mDNS & Audit")
_semaphore = asyncio.Semaphore(5)  # 限制最多同时 5 个请求去访问硬件

# 🌟 注意：全局超时时间调大到了 10 秒，防止首次 mDNS 解析耗时过长导致 HTTP 请求本身超时
_client = httpx.AsyncClient(timeout=10.0) 

DualCallback = Callable[[dict], Awaitable[None]]

# ==========================================
# 🌟 mDNS 解析模块（带并发去重与极速重试）
# ==========================================
_mdns_cache: Dict[str, dict] = {}
_mdns_lock = threading.Lock()
_parsing_locks: Dict[str, asyncio.Lock] = {}  # 🌟 并发去重锁
CACHE_TTL = 3600  # 缓存 1 小时

def _sync_mdns_resolve(hostname: str) -> Optional[str]:
    """同步执行 mDNS 解析（纯 Zeroconf 实现，自带微重试）"""
    # 直接使用 Zeroconf，跳过卡顿的系统原生 socket 解析
    zc = Zeroconf()
    try:
        base_name = hostname.replace('.local', '')
        service_name = f"{base_name}._http._tcp.local."
        
        # 🌟 微重试机制：尝试 3 次，每次等 800ms，比一次性等 1.5 秒更灵敏
        for attempt in range(3):
            try:
                info = zc.get_service_info("_http._tcp.local.", service_name, timeout=800)
                if info and info.parsed_addresses():
                    return info.parsed_addresses()[0]
            except Exception:
                pass
            time.sleep(0.2)  # 稍微等一下再重试
            
    except Exception as e:
        print(f"⚠️ [mDNS] Query failed for {hostname}: {e}")
    finally:
        zc.close()
    return None

async def _resolve_client_url(client_url: str) -> tuple[str, bool, str]:
    """解析 URL，返回: (解析后的URL, 是否使用了mDNS, 解析出的IP)"""
    if ".local" not in client_url:
        return client_url, False, ""

    hostname = client_url.split("//")[-1].split("/")[0].split(":")[0]
    current_time = time.time()

    # 1. 检查缓存
    with _mdns_lock:
        cached = _mdns_cache.get(hostname)
        if cached and (current_time - cached["time"] < CACHE_TTL):
            return client_url.replace(hostname, cached["ip"]), True, cached["ip"]

    # 2. 🌟 并发去重：防止前端一瞬间发 10 个请求，导致发 10 次 UDP 广播
    if hostname not in _parsing_locks:
        _parsing_locks[hostname] = asyncio.Lock()

    async with _parsing_locks[hostname]:
        # 双重检查（Double-Checked Locking）：可能在等锁的时候，别人已经解析好了
        with _mdns_lock:
            cached = _mdns_cache.get(hostname)
            if cached and (time.time() - cached["time"] < CACHE_TTL):
                return client_url.replace(hostname, cached["ip"]), True, cached["ip"]

        # 3. 真正执行解析（放入线程池，绝不阻塞主事件循环）
        resolved_ip = await asyncio.to_thread(_sync_mdns_resolve, hostname)
        
        if resolved_ip:
            with _mdns_lock:
                _mdns_cache[hostname] = {"ip": resolved_ip, "time": time.time()}
            print(f"✅ [mDNS] Resolved & Cached: {hostname} -> {resolved_ip}")
            return client_url.replace(hostname, resolved_ip), True, resolved_ip
        else:
            print(f"❌ [mDNS] Resolution Failed after retries: {hostname}")
            raise ConnectionError(f"mDNS resolution failed for {hostname}. Ensure the device is online.")


# ==========================================
# 🌟 核心路由装饰器 (代码保持不变)
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
            "path": path,
            "method": method,
            "original_client_url": client_url,
            "resolved_client_url": client_url,
            "mdns_resolved": False,
            "resolved_ip": "",
            "client_ip": request.client.host if request.client else "unknown",
            "query_params": dict(request.query_params),
            "esp_status": 0,
            "esp_data": None,
            "result": None,
            "error": None,
            "elapsed_ms": 0
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
                resp = await _client.request(
                    method=method.upper(),
                    url=full_url,
                    content=req_body,
                    headers=req_headers
                )
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
            if cb:
                await _safe_call_cb(cb, ctx)

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