from enum import IntEnum
from atools.object_tools import Object

from ..base.reader import NetReader


class KcpEvent(IntEnum):
    SEND = 1
    RECEIVE = 2


class KcpCommand(IntEnum):
    PUSH = 81
    ACK = 82
    WASK = 83
    WINS = 84


class KcpSegment(Object):
    conv: int
    cmd: KcpCommand
    frg: int
    wnd: int
    ts: int
    sn: int
    una: int
    body: bytes

    @staticmethod
    def decode(_: bytes | NetReader) -> KcpSegment:
        if not isinstance(_, NetReader):
            _ = NetReader(_)
        if _.size - _.offset < 24:
            raise Exception("Unrecognized KCP header")
        kcp_segment = KcpSegment()
        kcp_segment.conv = _.u32

        cmd = _.u8
        try:
            kcp_segment.cmd = KcpCommand(cmd)
        except ValueError:
            raise ValueError(f"Unknown KCP command：{cmd}")

        kcp_segment.frg = _.u8
        kcp_segment.wnd = _.u16
        kcp_segment.ts = _.u32
        kcp_segment.sn = _.u32
        kcp_segment.una = _.u32
        body_size = _.u32

        if kcp_segment.cmd != KcpCommand.PUSH and (kcp_segment.frg != 0 or body_size != 0):
            raise Exception("KCP control segments cannot have fragments or payload")
        if _.size - _.offset < body_size:
            raise Exception("Truncated KCP payload")
        kcp_segment.body = _.bytes(body_size)
        return kcp_segment


class KcpStream(Object):
    id: int
    conv: int
    event: KcpEvent
    src_address: str
    address: str
    chunk_id: int
    sn_start: int
    sn_end: int
    udp_packet_ids: tuple[int, ...]
    complete: bool
    body: bytes
