import time
from typing import Callable
from atools.logger import logger
from dataclasses import dataclass, field

from . import objects
from .base.reader import NetReader

_MASK = 0xFFFFFFFF
_HALF = 0x80000000


@dataclass(frozen=True)
class _KcpFlowKey:
    id: int
    conv: int
    client_address: str
    server_address: str


@dataclass(frozen=True)
class _KcpCaptured:
    segment: objects.KcpSegment
    packet_id: int


@dataclass
class _KcpDirection:
    next_sn: int
    boundary_known: bool
    pending: dict[int, _KcpCaptured] = field(default_factory=dict)
    buffered_bytes: int = 0


@dataclass
class _KcpFlow:
    last_seen: float
    next_chunk_id: int = 1
    directions: dict[objects.UdpEvent, _KcpDirection] = field(default_factory=dict)


class KcpReceiver:
    def __init__(
        self,
        *,
        start_sn: int | None = None,
        strict: bool = False,
        max_flows: int = 1024,
        max_pending_segments: int = 4096,
        max_pending_bytes: int = 8 * 1024 * 1024,
        max_total_bytes: int = 64 * 1024 * 1024,
        max_sequence_gap: int = 65535,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if start_sn is not None and not 0 <= start_sn <= _MASK:
            raise ValueError("start_sn must be a u32 or None")
        if min(max_flows, max_pending_segments, max_pending_bytes, max_total_bytes) <= 0:
            raise ValueError("resource limits must be positive")
        if not 0 < max_sequence_gap < _HALF:
            raise ValueError("max_sequence_gap must be between 1 and 2**31-1")
        self.start_sn = start_sn
        self.strict = strict
        self.max_flows = max_flows
        self.max_pending_segments = max_pending_segments
        self.max_pending_bytes = max_pending_bytes
        self.max_total_bytes = max_total_bytes
        self.max_sequence_gap = max_sequence_gap
        self.invalid_datagrams = 0
        self._clock = clock
        self._flows: dict[_KcpFlowKey, _KcpFlow] = {}
        self._buffered_bytes = 0

    @property
    def flow_count(self) -> int:
        return len(self._flows)

    @property
    def buffered_bytes(self) -> int:
        return self._buffered_bytes

    def feed(self, udp_stream: objects.UdpStream) -> list[objects.KcpStream]:
        event = objects.KcpEvent(udp_stream.event.value)
        try:
            _ = NetReader(udp_stream.body)
            conv = None
            kcp_segments = []
            while _.offset < _.size:
                kcp_segment = objects.KcpSegment.decode(_)
                if conv is not None and kcp_segment.conv != conv:
                    raise Exception("Mixed conv values in one datagram")
                conv = kcp_segment.conv
                kcp_segments.append(kcp_segment)
        except Exception as e:
            self.invalid_datagrams += 1
            if self.strict:
                raise
            logger.warning(f"{type(e).__name__}: {e}")
            return []
        kcp_flow_key = _KcpFlowKey(udp_stream.id, kcp_segments[0].conv, udp_stream.src_address, udp_stream.address)
        kcp_flow = self._flows.get(kcp_flow_key)
        pushes = [segment for segment in kcp_segments if segment.cmd == objects.KcpCommand.PUSH]
        if not pushes:
            if kcp_flow is not None:
                kcp_flow.last_seen = self._clock()
            return []
        if kcp_flow is None:
            if len(self._flows) >= self.max_flows:
                raise Exception("flow limit reached; close or expire old flows")
            kcp_flow = _KcpFlow(self._clock())
        previous = kcp_flow.directions.get(event)
        if previous is None:
            first_sn = self.start_sn
            if first_sn is None:
                first_sn = pushes[0].sn
                for segment in pushes[1:]:
                    if ((segment.sn - first_sn) & _MASK) > _HALF:
                        first_sn = segment.sn
            candidate = _KcpDirection(first_sn, self.start_sn is not None)
        else:
            candidate = _KcpDirection(previous.next_sn, previous.boundary_known, previous.pending.copy(), previous.buffered_bytes)
        previous_bytes = previous.buffered_bytes if previous else 0

        for segment in pushes:
            distance = (segment.sn - candidate.next_sn) & _MASK
            if distance > _HALF:
                continue
            if distance > self.max_sequence_gap:
                raise Exception("KCP sequence gap exceeds the configured receive window")
            existing = candidate.pending.get(segment.sn)
            if existing is not None:
                if (existing.segment.frg, existing.segment.body) != (segment.frg, segment.body):
                    raise Exception(f"conflicting retransmission at SN {segment.sn}")
                continue
            candidate.pending[segment.sn] = _KcpCaptured(segment, udp_stream.packet_id)
            candidate.buffered_bytes += len(segment.body)
        if len(candidate.pending) > self.max_pending_segments or candidate.buffered_bytes > self.max_pending_bytes:
            raise Exception("per-direction KCP buffer limit reached")
        if self._buffered_bytes - previous_bytes + candidate.buffered_bytes > self.max_total_bytes:
            raise Exception("total KCP buffer limit reached")

        result: list[objects.KcpStream] = []
        while candidate.next_sn in candidate.pending:
            start = candidate.next_sn
            first = candidate.pending[start].segment
            count = first.frg + 1
            fragments: list[_KcpCaptured] = []
            for index in range(count):
                item = candidate.pending.get((start + index) & _MASK)
                if item is None:
                    break
                if item.segment.frg != first.frg - index:
                    raise Exception(f"inconsistent KCP fragment countdown at SN {item.segment.sn}")
                fragments.append(item)
            if len(fragments) != count:
                break
            kcp_stream = objects.KcpStream()
            kcp_stream.id = udp_stream.id
            kcp_stream.conv = kcp_flow_key.conv
            kcp_stream.event = event
            kcp_stream.src_address = udp_stream.src_address
            kcp_stream.address = udp_stream.address
            kcp_stream.chunk_id = kcp_flow.next_chunk_id + len(result)
            kcp_stream.sn_start = start
            kcp_stream.sn_end = (start + count - 1) & _MASK
            kcp_stream.udp_packet_ids = tuple(dict.fromkeys(item.packet_id for item in fragments))
            kcp_stream.complete = candidate.boundary_known
            kcp_stream.body = b"".join(item.segment.body for item in fragments)
            result.append(kcp_stream)
            for item in fragments:
                del candidate.pending[item.segment.sn]
                candidate.buffered_bytes -= len(item.segment.body)
            candidate.next_sn = (start + count) & _MASK
            candidate.boundary_known = True

        self._buffered_bytes += candidate.buffered_bytes - previous_bytes
        kcp_flow.directions[event] = candidate
        kcp_flow.next_chunk_id += len(result)
        kcp_flow.last_seen = self._clock()
        self._flows[kcp_flow_key] = kcp_flow
        return result

    def _remove(self, key: _KcpFlowKey) -> None:
        flow = self._flows.pop(key)
        self._buffered_bytes -= sum(direction.buffered_bytes for direction in flow.directions.values())

    def close(self, udp_id: int) -> int:
        keys = [key for key in self._flows if key.id == udp_id]
        for key in keys:
            self._remove(key)
        return len(keys)

    def expire(self, max_idle_secs: float = 120) -> list[_KcpFlowKey]:
        if max_idle_secs <= 0:
            raise ValueError("max_idle_secs must be positive")
        now = self._clock()
        keys = [key for key, flow in self._flows.items() if now - flow.last_seen >= max_idle_secs]
        for key in keys:
            self._remove(key)
        return keys
