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

app = FastAPI(title="Dual Gateway", description="Built-in IoT Gateway")
_semaphore = asyncio.Semaphore(5)
_client = httpx.AsyncClient(timeout=3.0)

# 🌟 定义回调函数的类型签名 (接收一个字典，返回异步/同步皆可)
DualCallback = Callable[[dict], Awaitable[None]]

# ==========================================
# 🌟 mDNS 解析模块
# ==========================================
_mdns_cache: Dict[str, dict] = {} # 缓存结构: {hostname: {"ip": str, "time": float}}
_mdns_lock = threading.Lock()
CACHE_TTL = 60 # 缓存 60 秒

def _sync_mdns_resolve(hostname: str) -> Optional[str]:
    """同步执行 mDNS 解析（将在后台线程中运行）"""
    # 1. 尝试系统原生解析 (Linux/macOS 或安装了 Bonjour 的 Windows)
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        pass
    
    # 2. 使用 Zeroconf 组播查询
    zc = Zeroconf()
    try:
        # 查找局域网内的服务，常见的 HTTP 服务注册名
        service_name = f"{hostname.replace('.local', '')}._http._tcp.local."
        info = zc.get_service_info("_http._tcp.local.", service_name, timeout=1500) # 1.5秒超时
        if info and info.parsed_addresses():
            return info.parsed_addresses()[0]
    except Exception as e:
        print(f"⚠️ mDNS query failed for {hostname}: {e}")
    finally:
        zc.close()
    return None

async def resolve_client_url(client_url: str) -> tuple[str, bool, str]:
    """解析 URL，返回: (解析后的URL, 是否使用了mDNS, 解析出的IP)"""
    if ".local" not in client_url:
        return client_url, False, ""

    # 提取 hostname (例如从 http://tablebot.local:8080/api 提取 tablebot.local)
    hostname = client_url.split("//")[-1].split("/")[0].split(":")[0]
    current_time = time.time()

    # 检查缓存
    with _mdns_lock:
        cached = _mdns_cache.get(hostname)
        if cached and (current_time - cached["time"] < CACHE_TTL):
            return client_url.replace(hostname, cached["ip"]), True, cached["ip"]

    # 没有缓存，执行解析（放入线程池防止阻塞）
    resolved_ip = await asyncio.to_thread(_sync_mdns_resolve, hostname)
    
    if resolved_ip:
        with _mdns_lock:
            _mdns_cache[hostname] = {"ip": resolved_ip, "time": current_time}
        print(f"✅ mDNS Resolved: {hostname} -> {resolved_ip}")
        return client_url.replace(hostname, resolved_ip), True, resolved_ip
    else:
        print(f"❌ mDNS Resolution Failed: {hostname}")
        return client_url, True, ""

# ==========================================
# 路由与核心逻辑
# ==========================================
def route(path: str, method: str = "GET", client_url: str = "", cb: Optional[DualCallback] = None):
    """声明式双面路由装饰器（支持全链路回调与 mDNS）"""

    def decorator(func):
        endpoint = _create_endpoint(func, path, method, client_url, cb)
        app.add_api_route(path, endpoint, methods=[method])
        return func

    return decorator

def _create_endpoint(func, path, method, client_url, cb):
    async def endpoint(request: Request):
        start_time = time.perf_counter()

        # 用于收集上下文的字典
        ctx = {
            "path": path,
            "method": method,
            "original_client_url": client_url, # 原始 URL (可能是 .local)
            "resolved_client_url": client_url, # 解析后的 URL (真实的 IP)
            "mdns_resolved": False,            # 是否触发了 mDNS 解析
            "resolved_ip": "",                 # 解析出的真实 IP
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
            # 🌟 1. 动态解析 mDNS
            resolved_url, is_mdns, ip = await resolve_client_url(client_url)
            ctx["resolved_client_url"] = resolved_url
            ctx["mdns_resolved"] = is_mdns
            ctx["resolved_ip"] = ip

            if is_mdns and not ip:
                ctx["error"] = "mDNS Resolution Failed"
                ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                if cb: await _safe_call_cb(cb, ctx)
                return JSONResponse(status_code=502, content={"error": "Failed to resolve hardware IP via mDNS"})

            # 2. 组装完整 URL
            full_url = f"{resolved_url.rstrip('/')}{path}"
            if request.query_params:
                full_url += f"?{request.url.query}"

            req_body = None
            req_headers = {}
            if method.upper() in ["POST", "PUT", "PATCH"]:
                req_body = await request.body()
                if "content-type" in request.headers:
                    req_headers["Content-Type"] = request.headers["content-type"]

            # 3. 请求硬件
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

            # 4. 智能解析
            esp_data = raw_text
            try:
                esp_data = json.loads(raw_text)
            except json.JSONDecodeError:
                pass
            ctx["esp_data"] = esp_data

            # 5. 执行本地业务
            try:
                result = await func(esp_data)
                ctx["result"] = result
            except Exception as e:
                ctx["error"] = f"Local handler error: {str(e)}"
                if cb:
                    ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                    await _safe_call_cb(cb, ctx)
                return JSONResponse(status_code=500, content={"error": ctx["error"]})

            # 6. 计算总耗时并触发回调
            ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
            if cb:
                await _safe_call_cb(cb, ctx)

            # 7. 智能响应
            if isinstance(result, str):
                if result.strip().startswith("<"):
                    return Response(content=result, media_type="text/html")
                return Response(content=result, media_type="text/plain")

            return result

    return endpoint

async def _safe_call_cb(cb, ctx):
    """安全执行回调，防止回调里的异常导致网关崩溃"""
    try:
        await cb(ctx)
    except Exception as e:
        print(f"⚠️ Callback execution failed: {e}")