import json
import asyncio
import aiofiles
from pathlib import Path
from datetime import datetime
from atools.logger import logger

from .. import objects
from ..utils import url_to_name, content_encodings, decompress


class HttpCapture:
    def __init__(self, group_name: str | None = None, capture_dir: Path | str = None):
        if group_name is None:
            group_name = f"group-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        if capture_dir is None:
            capture_dir = Path(f"data/http-capture/{group_name}")
        else:
            capture_dir = Path(capture_dir)
        self.capture_path = capture_dir / group_name
        self.queues: dict[int, asyncio.Queue[objects.HttpStream]] = {}
        self.tasks: dict[int, asyncio.Task] = {}

    async def on_http(self, http_stream: objects.HttpStream):
        if http_stream.event == objects.HttpEvent.REQUEST and http_stream.id not in self.queues:
            self.queues[http_stream.id] = asyncio.Queue()
            self.tasks[http_stream.id] = asyncio.create_task(self.__save_http(http_stream.id, self.queues[http_stream.id]))
        queue = self.queues.get(http_stream.id)
        if queue is not None:
            queue.put_nowait(http_stream)

    async def __save_http(self, id: int, queue: asyncio.Queue[objects.HttpStream]):
        path: Path | None = None
        info_name = "request"
        headers: dict[str, dict[str, str]] = {}
        files = {}
        request_end = False
        try:
            while True:
                http_stream = await queue.get()
                event = http_stream.event
                if event in (objects.HttpEvent.REQUEST, objects.HttpEvent.RESPONSE):
                    info_name = "request" if event == objects.HttpEvent.REQUEST else "response"
                    if path is None:
                        path = self.capture_path / f"{id}_{url_to_name(http_stream.url)[:100]}"
                        await asyncio.to_thread(path.mkdir, parents=True, exist_ok=True)
                    headers[info_name] = http_stream.headers
                    async with aiofiles.open(path / info_name, "w", encoding="utf-8") as f:
                        await f.write(self.__info_doc(http_stream))
                    files[info_name] = await aiofiles.open(path / f"{info_name}_body", "ab")
                elif event in (objects.HttpEvent.REQUEST_BODY, objects.HttpEvent.RESPONSE_BODY):
                    f = files.get("request" if event == objects.HttpEvent.REQUEST_BODY else "response")
                    if f is not None:
                        await f.write(http_stream.body)
                        if queue.empty():
                            await f.flush()
                elif event in (objects.HttpEvent.REQUEST_END, objects.HttpEvent.RESPONSE_END):
                    peer_name = "request" if event == objects.HttpEvent.REQUEST_END else "response"
                    if (f := files.pop(peer_name, None)) is not None:
                        await f.close()
                        if encodings := content_encodings(headers[peer_name]):
                            await asyncio.to_thread(self.__decompress, path / f"{peer_name}_body", encodings)
                    if event == objects.HttpEvent.RESPONSE_END:
                        break
                    request_end = True
                elif event == objects.HttpEvent.ERROR:
                    async with aiofiles.open(path / info_name, "a", encoding="utf-8") as f:
                        await f.write(f"\nError: {json.dumps(http_stream.error)}")
                    if request_end:
                        break
                    request_end = True
        except Exception:
            logger.exception("Failed to save HTTP capture %s", id)
        finally:
            for f in files.values():
                await f.close()
            self.queues.pop(id, None)
            self.tasks.pop(id, None)

    @staticmethod
    def __info_doc(http_stream: objects.HttpStream) -> str:
        info_doc = []
        info_doc.append(f"Url: {json.dumps(http_stream.url)}")
        info_doc.append(f"Method: {http_stream.method.name}")
        if http_stream.status is not None:
            info_doc.append(f"Status: {http_stream.status}")
        info_doc.append(f"Headers:")
        for name, value in http_stream.headers.items():
            info_doc.append(f"- {json.dumps(name)}: {json.dumps(value)}")
        return "\n".join(info_doc)

    @staticmethod
    def __decompress(path: Path, encodings: list[str]):
        try:
            body = decompress(path.read_bytes(), encodings)
        except ValueError as exc:
            logger.warning("Failed to decompress %s: %s", path, exc)
            return
        path.write_bytes(body)
