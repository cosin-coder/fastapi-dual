# 🚀 FastAPI-Dual

A lightweight, high-performance **Dual-Route gateway** for IoT and Edge Computing, inspired by Ktor & Go. 

## ✨ 特性
- 🪄 **装饰器声明**：像写 Flask 一样极简。
- 🚦 **异步背压**：内置 `asyncio.Semaphore` 保护脆弱的 ESP32 等微控制器。
- 🔄 **参数透传**：自动将前端 Query 和 Body 转发给硬件。
- 🧠 **智能响应**：自动识别 HTML 并渲染，或自动将 Dict 序列化为 JSON。
- 🔍 **全链路回调**：支持通过 `cb` 参数注入请求、硬件状态等审计信息。

## 📦 安装 (Installation)

因为暂未发布至 PyPI，你可以直接通过 GitHub 安装：

```bash
pip install git+https://github.com/cosin-coder/fastapi-dual.git