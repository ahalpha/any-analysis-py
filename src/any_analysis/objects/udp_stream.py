from enum import IntEnum
from atools.object_tools import Object

from ..base.reader import NetReader
from ..base.writer import NetWriter


class UdpEvent(IntEnum):
    SEND = 1
    RECEIVE = 2


class UdpStream(Object):
    id: int
    event: UdpEvent
    src_address: str
    address: str
    packet_id: int
    body: bytes

    @staticmethod
    def decode(_: NetReader | bytes) -> UdpStream:
        if not isinstance(_, NetReader):
            _ = NetReader(_)
        udp_stream = UdpStream()
        udp_stream.id = _.varint
        udp_stream.event = UdpEvent(_.u8)
        udp_stream.src_address = _.str(_.varint)
        udp_stream.address = _.str(_.varint)
        udp_stream.packet_id = _.varint
        udp_stream.body = _.bytes(_.varint)
        return udp_stream

    def encode(self, _: NetWriter):
        _.varint(self.id)
        _.u8(self.event.value)
        c = self.src_address.encode()
        _.varint(len(c))
        _.bytes(c)
        c = self.address.encode()
        _.varint(len(c))
        _.bytes(c)
        _.varint(self.packet_id)
        _.varint(len(self.body))
        _.bytes(self.body)
