# any-analysis-py
a backend analysis interface for `any-capture`, designed to simplify packet analysis.

```python
from any_analysis import AnyCapture, objects
from any_analysis.templates import HttpCapture, TcpCapture, UdpCapture

capture = AnyCapture(secret="<secret>")
http_capture = HttpCapture()
tcp_capture = TcpCapture()
udp_capture = UdpCapture()


@capture.on_http_event()
async def on_http(http_stream: objects.HttpStream):
    await http_capture.on_http(http_stream)


@capture.on_tcp_event()
async def on_tcp(tcp_stream: objects.TcpStream):
    await tcp_capture.on_tcp(tcp_stream)


@capture.on_udp_event()
async def on_udp(udp_stream: objects.UdpStream):
    await udp_capture.on_udp(udp_stream)


@capture.on_ws_event()
async def on_ws(ws_stream: objects.WsStream):
    print(f"id={ws_stream.id}, frame_id={ws_stream.frame_id}, event={ws_stream.event.name}, opcode={ws_stream.opcode.name}, body={ws_stream.body}")


if __name__ == "__main__":
    capture.run()

```
