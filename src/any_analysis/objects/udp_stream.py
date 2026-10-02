from enum import IntEnum
from atools.object_tools import Object

from ..base.reader import NetReader


class UdpEvent(IntEnum):
    SEND = 1
    RECEIVE = 2


class UdpStream(Object):
    id: int
    event: UdpEvent
    src_address: str
    address: str
    chunk_id: int
    body: bytes

    @staticmethod
    def decode(buf: bytes) -> UdpStream:
        _ = NetReader(buf)
        udp_stream = UdpStream()
        udp_stream.id = _.varint
        udp_stream.event = UdpEvent(_.u8)
        udp_stream.src_address = _.str(_.varint)
        udp_stream.address = _.str(_.varint)
        udp_stream.packet_id = _.varint
        udp_stream.body = _.bytes(_.varint)
        return udp_stream
