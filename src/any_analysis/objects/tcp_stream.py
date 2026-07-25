from enum import IntEnum
from atools.object_tools import Object

from ..base.reader import NetReader


class TcpEvent(IntEnum):
    ABOUT = 1
    OK = 2
    SEND = 3
    RECEIVE = 4
    CLIENT_CLOSE = 5
    SERVER_CLOSE = 6
    DISCONNECT = 7


class TcpStream(Object):
    id: int
    event: TcpEvent
    address: str
    chunk_id: int
    body: bytes

    @staticmethod
    def decode(buf: bytes) -> TcpStream:
        _ = NetReader(buf)
        tcp_stream = TcpStream()
        tcp_stream.id = _.varint
        tcp_stream.event = TcpEvent(_.u8)
        tcp_stream.address = _.str(_.varint)
        tcp_stream.chunk_id = _.varint
        tcp_stream.body = _.bytes(_.varint)
        return tcp_stream
