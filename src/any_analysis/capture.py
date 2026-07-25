import typing
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from . import objects


class AnyCapture:
    def __init__(self, host="127.0.0.1", port=8900):
        self.host = host
        self.port = port
        self.app = FastAPI()
        self.http_handler = None
        self.tcp_handler = None

        @self.app.post("/http")
        async def __on_http_event(request: Request):
            if self.http_handler is None:
                return Response(status_code=204)
            http_stream = objects.HttpStream.decode(await request.body())
            await self.http_handler(http_stream)
            return Response(status_code=204)

        @self.app.post("/tcp")
        async def __on_tcp_event(request: Request):
            if self.tcp_handler is None:
                return Response(status_code=204)
            tcp_stream = objects.TcpStream.decode(await request.body())
            await self.tcp_handler(tcp_stream)
            return Response(status_code=204)

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

    def run(self):
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            yield

        self.app.router.lifespan_context = lifespan
        uvicorn.run(self.app, host=self.host, port=self.port, access_log=False)
