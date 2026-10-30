import asyncio
import json
import time
import httpx
import socket
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
_client = httpx.AsyncClient(timeout=3.0) # 全局 HTTP 客户端，复用连接池

# 定义回调函数的类型签名
DualCallback = Callable[[dict], Awaitable[None]]

# ==========================================
# 🌟 mDNS 解析模块（带长效缓存）
# ==========================================
_mdns_cache: Dict[str, dict] = {}
_mdns_lock = threading.Lock()
CACHE_TTL = 3600  # 缓存 1 小时

def _sync_mdns_resolve(hostname: str) -> Optional[str]:
    """同步执行 mDNS 解析（将在后台线程中运行，避免阻塞主事件循环）"""
    # 1. 尝试系统原生解析 (最快)
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        pass
    
    # 2. 使用 Zeroconf 组播查询
    zc = Zeroconf()
    try:
        # 查找局域网内的 HTTP 服务注册
        service_name = f"{hostname.replace('.local', '')}._http._tcp.local."
        info = zc.get_service_info("_http._tcp.local.", service_name, timeout=1500)
        if info and info.parsed_addresses():
            return info.parsed_addresses()[0]
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

    # 检查缓存
    with _mdns_lock:
        cached = _mdns_cache.get(hostname)
        if cached and (current_time - cached["time"] < CACHE_TTL):
            return client_url.replace(hostname, cached["ip"]), True, cached["ip"]

    # 缓存未命中，执行解析
    resolved_ip = await asyncio.to_thread(_sync_mdns_resolve, hostname)
    
    if resolved_ip:
        with _mdns_lock:
            _mdns_cache[hostname] = {"ip": resolved_ip, "time": current_time}
        print(f"✅ [mDNS] Resolved & Cached: {hostname} -> {resolved_ip}")
        return client_url.replace(hostname, resolved_ip), True, resolved_ip
    else:
        print(f"❌ [mDNS] Resolution Failed: {hostname}")
        # 🌟 关键：解析失败立刻抛出异常，触发 Fail-fast
        raise ConnectionError(f"mDNS resolution failed for {hostname}. Ensure the device is online.")


# ==========================================
# 🌟 核心路由装饰器
# ==========================================
def route(path: str, method: str = "GET", client_url: str = "", cb: Optional[DualCallback] = None):
    """声明式双面路由装饰器"""
    def decorator(func):
        endpoint = _create_endpoint(func, path, method, client_url, cb)
        app.add_api_route(path, endpoint, methods=[method])
        return func
    return decorator


def _create_endpoint(func, path, method, client_url, cb):
    async def endpoint(request: Request):
        start_time = time.perf_counter()

        # 用于全链路审计的上下文
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

        # 1. 并发背压控制
        if _semaphore.locked():
            ctx["error"] = "ESP32_BUSY"
            ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
            if cb: await _safe_call_cb(cb, ctx)
            return JSONResponse(status_code=429, content={"error": "Too many requests"})

        async with _semaphore:
            # 2. mDNS 动态解析 (Fail-fast)
            try:
                resolved_url, is_mdns, ip = await _resolve_client_url(client_url)
                ctx["resolved_client_url"] = resolved_url
                ctx["mdns_resolved"] = is_mdns
                ctx["resolved_ip"] = ip
            except ConnectionError as e:
                ctx["error"] = str(e)
                ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                if cb: await _safe_call_cb(cb, ctx)
                # 直接返回 502，绝不等 HTTP 超时
                return JSONResponse(status_code=502, content={"error": str(e)})

            # 3. 组装目标 URL 并透传 Query 参数
            full_url = f"{resolved_url.rstrip('/')}{path}"
            if request.query_params:
                full_url += f"?{request.url.query}"

            # 4. 透传 Body 和 Headers (针对 POST/PUT)
            req_body = None
            req_headers = {}
            if method.upper() in ["POST", "PUT", "PATCH"]:
                req_body = await request.body()
                if "content-type" in request.headers:
                    req_headers["Content-Type"] = request.headers["content-type"]

            # 5. 请求硬件
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

            # 6. 智能解析硬件返回 (JSON 或 纯文本/HTML)
            esp_data = raw_text
            try:
                esp_data = json.loads(raw_text)
            except json.JSONDecodeError:
                pass
            ctx["esp_data"] = esp_data

            # 7. 执行业务逻辑
            try:
                result = await func(esp_data)
                ctx["result"] = result
            except Exception as e:
                ctx["error"] = f"Local handler error: {str(e)}"
                if cb:
                    ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                    await _safe_call_cb(cb, ctx)
                return JSONResponse(status_code=500, content={"error": ctx["error"]})

            # 8. 计算耗时并触发全链路回调
            ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
            if cb:
                await _safe_call_cb(cb, ctx)

            # 9. 智能响应引擎
            if isinstance(result, str):
                if result.strip().startswith("<"):
                    return Response(content=result, media_type="text/html")
                return Response(content=result, media_type="text/plain")

            return result

    return endpoint


async def _safe_call_cb(cb, ctx):
    """安全执行回调，防止回调异常导致网关崩溃"""
    try:
        await cb(ctx)
    except Exception as e:
        print(f"⚠️ Callback execution failed: {e}")