"""Forensic Binary Archiver (SHA-256) & Partitioned Parquet Writer."""

import hashlib
import json
from pathlib import Path
import struct
from typing import Iterator, List

import pandas as pd

from recorder.models import OrderBookSnapshot

FRAME_STRUCT = struct.Struct(">IQQ")  # payload_len, wall_ns, mono_ns


class BinaryFrameArchiver:
    """Append-only binary frame archiver with streaming SHA-256 verification."""

    def __init__(self, archive_path: Path | str) -> None:
        self.path = Path(archive_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._hasher = hashlib.sha256()
        self._file = open(self.path, "wb")
        self.frame_count = 0

    def write_frame(self, payload: bytes, wall_ns: int, mono_ns: int) -> None:
        header = FRAME_STRUCT.pack(len(payload), wall_ns, mono_ns)
        self._file.write(header)
        self._file.write(payload)
        self._hasher.update(header)
        self._hasher.update(payload)
        self.frame_count += 1

    def close(self) -> str:
        self._file.flush()
        self._file.close()
        return self._hasher.hexdigest()

    @staticmethod
    def read_frames(archive_path: Path | str) -> Iterator[bytes]:
        with open(archive_path, "rb") as f:
            while True:
                hdr = f.read(FRAME_STRUCT.size)
                if not hdr or len(hdr) < FRAME_STRUCT.size:
                    break
                length, _, _ = FRAME_STRUCT.unpack(hdr)
                data = f.read(length)
                if len(data) < length:
                    break
                yield data


class ParquetBookWriter:
    """Writes normalized L2 snapshots to Snappy-compressed Parquet."""

    def __init__(self, output_dir: Path | str) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._buffer: List[dict] = []

    def write(self, snap: OrderBookSnapshot) -> None:
        self._buffer.append({
            "symbol": snap.symbol,
            "exchange_time_ns": snap.exchange_time_ns,
            "receive_time_ns": snap.receive_time_ns,
            "receive_monotonic_ns": snap.receive_monotonic_ns,
            "mid_price": snap.mid_price,
            "spread": snap.spread,
            "bid_prices": snap.bid_prices,
            "bid_quantities": snap.bid_quantities,
            "ask_prices": snap.ask_prices,
            "ask_quantities": snap.ask_quantities,
        })

    def flush(self, filename: str = "books.parquet") -> Path:
        out_path = self.output_dir / filename
        df = pd.DataFrame(self._buffer)
        df.to_parquet(out_path, index=False, compression="snappy")
        self._buffer.clear()
        return out_path
