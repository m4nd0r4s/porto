# Security policy

porto and douro are pre-alpha: nothing has been released yet, and no version is fit for
production. This policy applies from the first release on.

## Reporting a vulnerability

Please report security issues privately through GitHub: on the repository's **Security** tab,
choose **Report a vulnerability**. Do not open a public issue for them.

Include what you found, how to reproduce it, and the versions of porto, douro and Python you
used. You will get an answer within a week, and a fix or a plan within thirty days for
confirmed issues.

## Scope

In scope, for example:

- HTTP parsing flaws in douro, including request smuggling (`Transfer-Encoding` together with
  `Content-Length`, duplicate `Content-Length` values, obs-fold) and HTTP/2 stream abuse that
  gets past the reset-rate limit;
- bypassing one of the `douro.Limits` defaults (header, URI, body, JSON, form, multipart,
  timeout, concurrency, HTTP/2 or WebSocket limits) without the application raising or
  disabling that limit;
- path traversal in `File` or `StaticFiles`: serving a file outside `root=`, or following a
  symlink out of the root when `follow_symlinks=False`;
- TLS misconfiguration in a default setup, such as weak protocol versions or ciphers, or a
  certificate that is not checked;
- crashes or memory safety issues in the native code (`porto-core`, `porto-py` and the
  `porto-abi` C ABI) triggered by data or by Python code that uses only the public API;
- the debug console (`console=`) listening on any address other than the one configured.

Out of scope:

- running untrusted Python code: application code runs with the same rights as any script you
  run, and porto does not sandbox it;
- denial of service that needs more than the documented limits allow, or limits the
  application raised or turned off itself;
- code paths that are not in a release yet (unmerged branches, unreleased features, the main
  branch between releases).

## Supported versions

Until 1.0, security fixes go into the latest 0.x release. From 1.0 on, they go into the latest
minor release.
