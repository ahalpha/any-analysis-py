import zlib
import gzip
import brotli
import zstandard
from enum import IntEnum
from atools.logger import logger
from atools.object_tools import Object

from ..base.reader import NetReader
from ..base.writer import NetWriter


class HttpEvent(IntEnum):  # Enum style modeled after Rust
    Request = 1
    Response = 2
    Error = 3
    RequestBody = 4
    ResponseBody = 5
    RequestEnd = 6
    ResponseEnd = 7


class HttpMethod(IntEnum):
    Get = 1
    Post = 2
    Put = 3
    Delete = 4
    Head = 5
    Options = 6
    Connect = 7
    Patch = 9
    Other = 255


class HttpStream(Object):
    id: int
    event: HttpEvent
    method: HttpMethod | None
    url: str | None
    status: int | None
    headers: dict[str, str] | None
    chunk_id: int
    body: bytes | None
    error: str | None

    @staticmethod
    def decode(_: NetReader | bytes) -> HttpStream:
        if not isinstance(_, NetReader):
            _ = NetReader(_)
        http_stream = HttpStream()
        http_stream.id = _.varint
        http_stream.event = HttpEvent(_.u8)

        if http_stream.event in (HttpEvent.Request, HttpEvent.Response):
            http_stream.method = HttpMethod(_.u8)
            http_stream.url = _.str(_.varint)
            http_stream.headers = {}
            for _i_ in range(_.varint):
                key = _.str(_.varint)
                val = _.str(_.varint)
                http_stream.headers[key] = val
        else:
            http_stream.method = None
            http_stream.url = None
            http_stream.headers = None

        if http_stream.event == HttpEvent.Response:
            http_stream.status = _.varint
        else:
            http_stream.status = None

        if http_stream.event == HttpEvent.Error:
            http_stream.error = _.str(_.varint)
        else:
            http_stream.error = None

        if http_stream.event in (HttpEvent.RequestBody, HttpEvent.ResponseBody):
            http_stream.chunk_id = _.varint
            http_stream.body = _.bytes(_.varint)
        else:
            http_stream.chunk_id = None
            http_stream.body = None
        return http_stream

    def encode(self, _: NetWriter = None):
        if _ is None:
            _ = NetWriter()
            f = True
        else:
            f = False
        def write_str(s: str | None):
            c = (s or "").encode()
            _.varint(len(c))
            _.bytes(c)
        _.varint(self.id)
        _.u8(self.event.value)
        if self.event in (HttpEvent.Request, HttpEvent.Response):
            _.u8(self.method.value)
            write_str(self.url)
            headers = self.headers or {}
            _.varint(len(headers))
            for key, val in headers.items():
                write_str(key)
                write_str(val)
        if self.event == HttpEvent.Response:
            _.varint(self.status if self.status is not None else 0)
        if self.event == HttpEvent.Error:
            write_str(self.error)
        if self.event in (HttpEvent.RequestBody, HttpEvent.ResponseBody):
            _.varint(self.chunk_id if self.chunk_id is not None else 0)
            body = self.body or b""
            if isinstance(body, str):
                body = body.encode()
            _.varint(len(body))
            _.bytes(body)
        if f:
            data = _.getvalue()
            _.close()
            return data

    def auto_decompress(self):
        content_encoding = next((value for key, value in self.headers.items() if key.lower() == "content-encoding"), "")
        encodings = [encoding.strip().lower() for encoding in content_encoding.split(",") if encoding.strip()]
        for encoding in reversed(encodings):
            if encoding in {"gzip", "x-gzip"}:
                try:
                    self.body = gzip.decompress(self.body)
                except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
                    raise ValueError("Invalid gzip-compressed HTTP body") from exc
            if encoding == "br":
                self.body = brotli.decompress(self.body)
            if encoding in {"zstd"}:
                self.body = zstandard.ZstdDecompressor().decompressobj().decompress(self.body)
