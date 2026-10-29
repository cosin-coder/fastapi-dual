import asyncio
import json
import time
import httpx
from typing import Callable, Awaitable, Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

app = FastAPI(title="Dual Gateway", description="Built-in IoT Gateway")
_semaphore = asyncio.Semaphore(5)
_client = httpx.AsyncClient(timeout=3.0)

# 🌟 定义回调函数的类型签名 (接收一个字典，返回异步/同步皆可)
DualCallback = Callable[[dict], Awaitable[None]]


def route(path: str, method: str = "GET", client_url: str = "", cb: Optional[DualCallback] = None):
    """声明式双面路由装饰器（支持全链路回调）"""

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
            "client_url": client_url,
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
            full_url = f"{client_url.rstrip('/')}{path}"
            if request.query_params:
                full_url += f"?{request.url.query}"

            req_body = None
            req_headers = {}
            if method.upper() in ["POST", "PUT", "PATCH"]:
                req_body = await request.body()
                if "content-type" in request.headers:
                    req_headers["Content-Type"] = request.headers["content-type"]

            # 请求硬件
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

            # 智能解析
            esp_data = raw_text
            try:
                esp_data = json.loads(raw_text)
            except json.JSONDecodeError:
                pass
            ctx["esp_data"] = esp_data

            # 执行本地业务
            try:
                result = await func(esp_data)
                ctx["result"] = result
            except Exception as e:
                ctx["error"] = f"Local handler error: {str(e)}"
                if cb:
                    ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
                    await _safe_call_cb(cb, ctx)
                return JSONResponse(status_code=500, content={"error": ctx["error"]})

            # 计算总耗时并触发回调
            ctx["elapsed_ms"] = (time.perf_counter() - start_time) * 1000
            if cb:
                await _safe_call_cb(cb, ctx)

            # 智能响应
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