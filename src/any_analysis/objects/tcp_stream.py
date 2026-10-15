from enum import IntEnum
from atools.object_tools import Object

from ..base.reader import NetReader
from ..base.writer import NetWriter


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
    src_address: str
    address: str
    chunk_id: int
    body: bytes

    @staticmethod
    def decode(_: NetReader | bytes) -> TcpStream:
        if not isinstance(_, NetReader):
            _ = NetReader(_)
        tcp_stream = TcpStream()
        tcp_stream.id = _.varint
        tcp_stream.event = TcpEvent(_.u8)
        tcp_stream.src_address = _.str(_.varint)
        tcp_stream.address = _.str(_.varint)
        tcp_stream.chunk_id = _.varint
        tcp_stream.body = _.bytes(_.varint)
        return tcp_stream

    def encode(self, _: NetWriter):
        _.varint(self.id)
        _.u8(self.event.value)
        c = self.src_address.encode()
        _.varint(len(c))
        _.bytes(c)
        c = self.address.encode()
        _.varint(len(c))
        _.bytes(c)
        _.varint(self.chunk_id)
        _.varint(len(self.body))
        _.bytes(self.body)
