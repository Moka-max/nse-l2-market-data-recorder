"""Unit tests for L2 Market Data Recorder & Binary Archiver."""

import tempfile
from pathlib import Path
import unittest

import pandas as pd

from recorder.models import MockMarketDataAdapter
from recorder.archiver import BinaryFrameArchiver, ParquetBookWriter


class TestL2Recorder(unittest.TestCase):
    def test_end_to_end_recording_and_parity(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            archiver = BinaryFrameArchiver(tmp_path / "raw.bin")
            writer = ParquetBookWriter(tmp_path / "norm")
            feed = MockMarketDataAdapter()

            feed.add_handler(lambda s: (
                archiver.write_frame(s.symbol.encode("utf-8"), s.receive_time_ns, s.receive_monotonic_ns),
                writer.write(s),
            ))
            feed.start()
            for i in range(25):
                feed.emit_snapshot("NIFTY_FUT", mid_price=25400.0 + i, spread=5.0)
            feed.stop()

            digest = archiver.close()
            pq_file = writer.flush("nifty.parquet")

            self.assertEqual(len(digest), 64)
            frames = list(BinaryFrameArchiver.read_frames(tmp_path / "raw.bin"))
            self.assertEqual(len(frames), 25)

            df = pd.read_parquet(pq_file)
            self.assertEqual(len(df), 25)
            self.assertTrue((df["spread"] == 5.0).all())


if __name__ == "__main__":
    unittest.main()
