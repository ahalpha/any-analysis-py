import asyncio
import typing
import websockets
from atools.logger import logger

from . import objects


class AnyCapture:
    def __init__(self, capture_address: str = "127.0.0.1:8900", secret: str = None, reconnect_delay: float = 1.0):
        if not capture_address or not capture_address.strip():
            raise ValueError("capture_address must not be empty")
        if not secret:
            raise ValueError("secret must not be empty")
        if reconnect_delay < 0:
            raise ValueError("reconnect_delay must not be negative")

        address = capture_address.strip().rstrip("/")
        if not address.startswith(("ws://", "wss://")):
            address = f"ws://{address}"
        self.capture_address = address
        self.secret = secret
        self.reconnect_delay = reconnect_delay
        self.http_handler = None
        self.tcp_handler = None

    def on_http_event(self):
        def decorator(func: typing.Callable):
            self.http_handler = func
            return func

        return decorator

    def on_tcp_event(self):
        def decorator(func: typing.Callable):
            self.tcp_handler = func
            return func

        return decorator

    async def serve(self):
        await asyncio.gather(self.__consume("http"), self.__consume("tcp"))

    async def __consume(self, channel: str):
        uri = f"{self.capture_address}/{channel}"
        headers = {"Authorization": f"Bearer {self.secret}"}
        decoder = objects.HttpStream.decode if channel == "http" else objects.TcpStream.decode

        while True:
            try:
                async with websockets.connect(uri, additional_headers=headers) as websocket:
                    logger.info("Connected to capture WebSocket %s", uri)
                    async for payload in websocket:
                        if not isinstance(payload, bytes):
                            logger.warning("Ignored non-binary frame from %s", uri)
                            continue
                        try:
                            stream = decoder(payload)
                            handler = self.http_handler if channel == "http" else self.tcp_handler
                            if handler is not None:
                                await handler(stream)
                        except Exception:
                            logger.exception("Failed to process %s capture packet", channel)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Capture WebSocket %s disconnected: %s", uri, exc)
                await asyncio.sleep(self.reconnect_delay)

    def run(self):
        asyncio.run(self.serve())
