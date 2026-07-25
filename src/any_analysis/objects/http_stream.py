import zlib
import gzip
from enum import IntEnum
from atools.logger import logger
from atools.object_tools import Object

from ..base.reader import NetReader


class HttpEvent(IntEnum):
    REQUEST = 1
    RESPONSE = 2
    ERROR = 3


class HttpMethod(IntEnum):
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4
    HEAD = 5
    OPTIONS = 6
    CONNECT = 7
    PATCH = 8
    TRACE = 9
    OTHER = 255


class HttpStream(Object):
    id: int
    event: HttpEvent
    method: HttpMethod
    url: str
    status: int | None
    headers: dict[str, str]
    body: bytes

    @staticmethod
    def decode(buf: bytes) -> HttpStream:
        _ = NetReader(buf)
        http_stream = HttpStream()
        http_stream.id = _.varint
        http_stream.event = HttpEvent(_.u8)
        http_stream.method = HttpMethod(_.u8)
        http_stream.url = _.str(_.varint)
        if http_stream.event == HttpEvent.RESPONSE:
            http_stream.status = _.varint
        else:
            http_stream.status = None
        http_stream.headers = {}
        for _i_ in range(_.varint):
            key = _.str(_.varint)
            val = _.str(_.varint)
            http_stream.headers[key] = val
        http_stream.body = _.bytes(_.varint)
        if _.offset != _.size:
            logger.warning(f"package may be corrupted, {_.size - _.offset} trailing bytes.")
        return http_stream

    def auto_decompress(self):
        content_encoding = next((value for key, value in self.headers.items() if key.lower() == "content-encoding"), "")
        encodings = [encoding.strip().lower() for encoding in content_encoding.split(",") if encoding.strip()]
        for encoding in reversed(encodings):
            if encoding in {"gzip", "x-gzip"}:
                try:
                    self.body = gzip.decompress(self.body)
                except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
                    raise ValueError("Invalid gzip-compressed HTTP body") from exc
