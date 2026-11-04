# 39 — Test Client

```python
from douro.testing import Client

async def test_create_user():
    async with Client(app) as c:          # runs lifespan; in-process transport, no sockets
        r = await c.post("/users", json={"name": "Ana", "email": "a@x.io"})
        assert r.status == 201 and r.json()["name"] == "Ana"

def test_sync():
    with Client(app).sync() as c:          # starts a private porto runtime in a background thread via Portal
        assert c.get("/health").status == 200
```
- Transport modes: `"memory"` (default: requests are injected directly at the HTTP engine's request layer, bypassing sockets but going through the full Rust pipeline including layers), `"sim"` (over `porto.testing.net` for deterministic network tests), and `"tcp"` (real loopback server).
- API mirrors httpx: `get/post/put/patch/delete/request/stream`, `json=`, `data=`, `files=`, `headers=`, `cookies=` (cookie jar persisted), `follow_redirects`. Response: `status`, `headers`, `text`, `content`, `json()`, `http_version`.
- WebSocket: `async with c.websocket("/chat/1") as ws: await ws.send_text(...)`.
- `app.dependency_overrides` is honored. `Client(app, deterministic=True, seed=…)` runs under `porto.testing`.
- pytest plugin fixture `douro_client` is configured with `douro_app` (user fixture).
