# any-analysis-py
a backend analysis interface for `any-capture`, designed to simplify packet analysis.

```python
from any_analysis import AnyCapture

capture = AnyCapture("127.0.0.1:8900", "<secret>")


@capture.on_http_event()
async def on_http(stream):
    print(stream)


@capture.on_tcp_event()
async def on_tcp(stream):
    print(stream)


capture.run()
```
