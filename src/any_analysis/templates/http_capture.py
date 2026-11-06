import asyncio
import aiofiles
from pathlib import Path
from datetime import datetime

from .. import objects
from ..utils import url_to_name


class HttpCapture:
    def __init__(self, group_name: str | None = None, capture_dir: Path | str = None):
        if group_name is None:
            group_name = f"group-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        if capture_dir is None:
            capture_dir = Path(f"data/http-capture/{group_name}")
        else:
            capture_dir = Path(capture_dir)
        self.capture_path = capture_dir / group_name

    async def on_http(self, http_stream: objects.HttpStream):
        http_stream.auto_decompress()
        if http_stream.event == objects.HttpEvent.REQUEST:
            peer_name = "request"
        elif http_stream.event == objects.HttpEvent.RESPONSE:
            peer_name = "response"
        else:
            return
        url_name = url_to_name(http_stream.url)[:100]
        file_name = f"{http_stream.id}_{url_name}"

        info_doc = []
        info_doc.append(f"Url: {http_stream.url}")
        info_doc.append(f"Method: {http_stream.method.name}")
        info_doc.append(f"Headers:")
        for name, value in http_stream.headers.items():
            info_doc.append(f"- {name}: {value}")

        await asyncio.to_thread(Path(self.capture_path / file_name).mkdir, parents=True, exist_ok=True)

        async with aiofiles.open(self.capture_path / file_name / f"{peer_name}", "w", encoding="utf-8") as f:
            await f.write("\n".join(info_doc))

        async with aiofiles.open(self.capture_path / file_name / f"{peer_name}_body", "wb") as f:
            await f.write(http_stream.body)
