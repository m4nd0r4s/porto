# 36 — Responses, Streaming, WebSockets, SSE

## 1. Response classes (`douro.responses`, re-exported)
| Class | Notes |
|---|---|
| `Response(content: bytes \| str \| None = None, status=200, headers=None, media_type=None)` | base. `headers` is a `douro.Headers` multi-map (case-insensitive) |
| `JSON(data, status=200, headers=None, serializer=None)` | uses the generic encoder or the given compiled type |
| `Text`, `HTML`, `Redirect(url, status=307)`, `NoContent()` | |
| `File(path, *, filename=None, media_type=None, content_disposition="attachment", etag=True, last_modified=True)` | Range, If-None-Match, If-Modified-Since handled in Rust |
| `Stream(iterable: AsyncIterable[bytes \| str] \| Iterable[...], media_type=…)` | sync iterables are iterated on the blocking pool |
| `EventStream(source: AsyncIterable[douro.Event \| str \| dict], ping=15.0, retry=None)` | SSE; `Event(data, event=None, id=None, retry=None)`; dict → JSON data |
| `Problem(status, title=None, detail=None, type_=None, **extensions)` | RFC 9457 |

Mutating headers and cookies without building a full response: inject `resp: douro.Response` (a "sink") as a parameter, then call `resp.set_cookie(name, value, *, max_age, expires, path="/", domain=None, secure=True, httponly=True, samesite="lax")` or `resp.headers[...] = ...`. These merge into the final response.

## 2. Streaming semantics
- A streaming response's body producer runs in the request scope. Client disconnect → cancel (per `on_disconnect`). Backpressure: each yielded chunk is awaited until written to the socket buffer (bounded at 64 KiB in flight per stream).
- SSE: `Content-Type: text/event-stream`, `Cache-Control: no-cache`, comment pings every `ping` seconds while idle, and `Last-Event-ID` exposed as `request.headers`.

## 3. WebSockets
```python
@app.ws("/chat/{room}")
async def chat(ws: douro.WebSocket, room: str, user: Annotated[User, Depends(auth_ws)]) -> None:
    await ws.accept(subprotocol=None)
    async with porto.scope() as s:
        s.spawn(pump_out(ws, room))
        async for msg in ws:                 # str | bytes; ends on close
            await broadcast(room, msg)
```
- Engine: `tungstenite` protocol core in sans-io mode on the connection stream. It handles permessage-deflate (negotiated, configurable), fragmentation, ping/pong auto-reply, and close handshake with codes.
- API: `accept()`, `receive() -> str | bytes` (raises `douro.WebSocketDisconnect(code, reason)`), `receive_text()`, `receive_bytes()`, `receive_json(type=T)` (validated via the schema engine), `send(str|bytes)`, `send_json(obj)`, `close(code=1000, reason="")`, `ping()`, iteration, `ws.client_state`, `ws.headers`, `ws.query`.
- Limits: `max_message_size` (1 MiB), `max_frame_size`, `idle_timeout`, write buffer high-water mark (backpressure on `send`).
- HTTP/2 WebSockets (RFC 8441) are designed but deferred.
- Validation errors in WS dependencies → close with 1008 (policy violation).
- Conformance: Autobahn testsuite (S5).
