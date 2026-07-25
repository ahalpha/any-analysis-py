import asyncio
from . import objects


class ReliableTcpReceiver:
    def __init__(self, chunk_id: int):
        self.lock = asyncio.Lock()
        self.next_chunk_id = chunk_id
        self.pending_chunks: dict[int, objects.TcpStream] = {}

    def on_tcp_packet(self, tcp_stream: objects.TcpStream):
        ready = []
        if tcp_stream.chunk_id < self.next_chunk_id:
            return
        self.pending_chunks[tcp_stream.chunk_id] = tcp_stream
        while self.next_chunk_id in self.pending_chunks:
            ready.append(self.pending_chunks.pop(self.next_chunk_id))
            self.next_chunk_id += 1
        for ordered_stream in ready:
            yield ordered_stream
