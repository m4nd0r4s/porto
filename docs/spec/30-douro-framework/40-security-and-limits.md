# 40 — Security & Limits (secure by default)

## 1. `douro.Limits` defaults
| Limit | Default |
|---|---|
| `max_header_bytes` (total) | 16 KiB |
| `max_headers` | 100 |
| `max_uri_bytes` | 8 KiB |
| `max_body_size` | 1 MiB (per-route override; `None` = unlimited only when explicitly set) |
| `max_json_depth` | 64 |
| `max_form_fields` / `max_form_field_size` | 1000 / 1 MiB |
| `max_multipart_parts` / `max_file_size` | 100 / 10 MiB |
| `header_read_timeout` | 10 s |
| `body_read_idle_timeout` | 30 s |
| `keep_alive_timeout` | 5 s |
| `max_concurrent_requests_per_lane` | 1024 |
| `max_queue_wait` (load shedding) | 5 s → 503 |
| `h2_max_concurrent_streams` | 100 |
| `h2_reset_rate` | 100 / 10 s → GOAWAY |
| `ws_max_message_size` | 1 MiB |

## 2. Rules
- Path normalization as in §32 §6.
- No `Server` header by default. Error bodies never include tracebacks unless `debug=True`.
- Cookies default to `Secure; HttpOnly; SameSite=Lax`.
- Host header validation available (`TrustedHosts`). `request.url` uses the Host only after it is validated or trusted via ProxyHeaders.
- Request smuggling: rely on hyper's strict h1 parser. Reject `Transfer-Encoding` + `Content-Length` together, duplicate `Content-Length` values, and obs-fold.
- Multipart filenames are exposed raw and also as `safe_filename`, sanitized for path traversal.
- File responses: `File(path)` resolves the path relative to an explicit `root=` when given, and rejects symlinks escaping the root (`follow_symlinks=False` default for `StaticFiles`).
- Supply chain: `cargo-deny` and `cargo-audit` in CI, pinned GitHub Actions, wheels built with provenance (Trusted Publishing).
