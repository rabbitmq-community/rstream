import asyncio
import ssl
from typing import Optional

from . import schema
from .encoding import (
    decode_frame,
    encode_frame,
    encode_publish,
)

CHUNK_SIZE = 256
CONNECT_TIMEOUT = 3


class ConnectionClosed(Exception):
    ...


class Connection:
    def __init__(
        self,
        host: str,
        port: int,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        self.host = host
        self.port = port
        self._ssl_context = ssl_context

        self._buffer = bytearray()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def open(self) -> None:
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=self.host,
                    port=self.port,
                    ssl=self._ssl_context,
                ),
                timeout=CONNECT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise ConnectionError(f"Could not connect to {self.host}:{self.port}")

    async def close(self) -> None:
        assert self._writer is not None
        self._writer.close()
        await self._writer.wait_closed()

    async def _read(self, n: int) -> bytearray:
        assert self._reader is not None
        while len(self._buffer) < n:
            self._buffer += await self._reader.read(CHUNK_SIZE)
            if self._reader.at_eof():
                raise ConnectionClosed

        data = self._buffer[:n]
        del self._buffer[:n]
        return data

    async def _write_frame_raw(self, data: bytes) -> None:
        assert self._writer is not None
        self._writer.write(data)
        await self._writer.drain()

    async def _read_frame_raw(self) -> bytearray:
        length = int.from_bytes(await self._read(4), "big")
        return await self._read(length)

    async def write_frame(self, frame: schema.Frame, version: int = 1) -> None:
        await self._write_frame_raw(encode_frame(frame, version))

    async def write_frame_publish(self, frame: schema.Publish, version: int = 1) -> None:
        await self._write_frame_raw(encode_publish(frame, version))

    async def read_frame(self) -> schema.Frame:
        return decode_frame(await self._read_frame_raw())
