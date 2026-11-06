import zlib
import gzip
import brotli
import zstandard


def content_encodings(headers: dict[str, str]) -> list[str]:
    content_encoding = next((value for key, value in headers.items() if key.lower() == "content-encoding"), "")
    encodings = []
    for encoding in content_encoding.split(","):
        encoding = encoding.strip().lower()
        if encoding == "x-gzip":
            encoding = "gzip"
        if encoding in ("gzip", "br", "zstd"):
            encodings.append(encoding)
    return encodings


def decompress(body: bytes, encodings: list[str]) -> bytes:
    for encoding in reversed(encodings):
        if encoding == "gzip":
            try:
                body = gzip.decompress(body)
            except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
                raise ValueError("Invalid gzip-compressed HTTP body") from exc
        if encoding == "br":
            try:
                body = brotli.decompress(body)
            except brotli.error as exc:
                raise ValueError("Invalid br-compressed HTTP body") from exc
        if encoding == "zstd":
            try:
                body = zstandard.ZstdDecompressor().decompressobj().decompress(body)
            except zstandard.ZstdError as exc:
                raise ValueError("Invalid zstd-compressed HTTP body") from exc
    return body
