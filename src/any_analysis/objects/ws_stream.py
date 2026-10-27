from enum import IntEnum
from atools.object_tools import Object

from ..base.reader import NetReader
from ..base.writer import NetWriter


class WsEvent(IntEnum):
    OPEN = 1
    SEND = 2
    RECEIVE = 3
    DISCONNECT = 4


class WsOpcode(IntEnum):
    CONTINUATION = 0
    TEXT = 1
    BINARY = 2
    CLOSE = 8
    PING = 9
    PONG = 10


class WsStream(Object):
    id: int
    event: WsEvent
    url: str
    frame_id: int
    opcode: WsOpcode
    body: bytes

    @staticmethod
    def decode(_: NetReader | bytes) -> WsStream:
        if not isinstance(_, NetReader):
            _ = NetReader(_)
        ws_stream = WsStream()
        ws_stream.id = _.varint
        ws_stream.event = WsEvent(_.u8)
        ws_stream.url = _.str(_.varint)
        ws_stream.frame_id = _.varint
        ws_stream.opcode = WsOpcode(_.u8)
        ws_stream.body = _.bytes(_.varint)
        return ws_stream

    def encode(self, _: NetWriter = None):
        if _ is None:
            _ = NetWriter()
            f = True
        else:
            f = False
        _.varint(self.id)
        _.u8(self.event.value)
        c = self.url.encode()
        _.varint(len(c))
        _.bytes(c)
        _.varint(self.frame_id)
        _.u8(self.opcode.value)
        _.varint(len(self.body))
        _.bytes(self.body)
        if f:
            body = _.getvalue()
            _.close()
            return body
