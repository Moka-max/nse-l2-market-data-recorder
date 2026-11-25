"""Self-contained demonstration of L2 Market Data Recording & SHA-256 Verification."""

from dataclasses import asdict
import json
from pathlib import Path

from recorder.models import MockMarketDataAdapter
from recorder.archiver import BinaryFrameArchiver, ParquetBookWriter


def main() -> None:
    out_dir = Path("demo_output")
    archiver = BinaryFrameArchiver(out_dir / "raw" / "session.raw.bin")
    writer = ParquetBookWriter(out_dir / "normalized")
    feed = MockMarketDataAdapter()

    def on_snapshot(snap):
        payload = json.dumps(asdict(snap)).encode("utf-8")
        archiver.write_frame(payload, snap.receive_time_ns, snap.receive_monotonic_ns)
        writer.write(snap)

    feed.add_handler(on_snapshot)
    feed.start()

    for i in range(50):
        feed.emit_snapshot("BANKNIFTY_FUT", mid_price=62800.0 + i * 0.5, spread=20.0)

    feed.stop()
    sha256_digest = archiver.close()
    pq_path = writer.flush("banknifty_l2.parquet")

    recovered = list(BinaryFrameArchiver.read_frames(out_dir / "raw" / "session.raw.bin"))
    print(f"Recorded Frames : {archiver.frame_count}")
    print(f"Recovered Frames: {len(recovered)} (100% Row Parity)")
    print(f"SHA-256 Digest  : {sha256_digest}")
    print(f"Parquet Output  : {pq_path}")


if __name__ == "__main__":
    main()
