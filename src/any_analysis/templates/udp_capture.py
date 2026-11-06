import asyncio
import aiofiles
from pathlib import Path
from datetime import datetime
from any_analysis import objects


class UdpCapture:
    def __init__(self, group_name: str | None = None, capture_dir: Path | str = None):
        if group_name is None:
            group_name = f"group-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        if capture_dir is None:
            capture_dir = Path(f"data/udp-capture/{group_name}")
        else:
            capture_dir = Path(capture_dir)
        self.capture_path = capture_dir / group_name
        self.queues: dict[str, asyncio.Queue[bytes | None]] = {}
        self.tasks: dict[str, asyncio.Task] = {}

    async def on_udp(self, udp_stream: objects.UdpStream):
        file_name = f"{udp_stream.id}_{udp_stream.address.replace(':', '_').replace('.', '-')}.bin"
        if file_name not in self.queues:
            self.queues[file_name] = asyncio.Queue()
            self.tasks[file_name] = asyncio.create_task(self.__save_udp(file_name, self.queues[file_name]))
        queue = self.queues[file_name]
        queue.put_nowait(udp_stream.encode())

    async def __save_udp(self, file_name: str, queue: asyncio.Queue[bytes | None]):
        await asyncio.to_thread(self.capture_path.mkdir, parents=True, exist_ok=True)
        async with aiofiles.open(self.capture_path / file_name, "ab") as f:
            while (body := await queue.get()) is not None:
                await f.write(body)
                if queue.empty():
                    await f.flush()
