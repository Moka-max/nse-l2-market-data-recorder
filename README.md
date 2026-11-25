# NSE Level-2 Market Data Recorder & Deterministic Replay Engine

A production-grade, broker-agnostic Python framework for capturing, archiving, and replaying Level-2 (5-depth) market data with cryptographic integrity and nanosecond dual-timestamping.

## Key Engineering Features
- **Broker-Agnostic Adapter Pattern:** Clean `BaseMarketDataAdapter` interface (plug-and-play for Shoonya, Zerodha Kite, Interactive Brokers, or simulated feeds).
- **Forensic Binary Archiving:** Append-only framed binary stream (`.raw.bin`) with streaming **SHA-256** verification.
- **Dual-Timestamp Precision:** Captures both `exchange_time_ns` and `receive_monotonic_ns` for latency and clock-drift auditing.
- **Partitioned Parquet Storage:** Writes clean, Snappy-compressed Apache Parquet files partitioned by symbol.
- **Zero-Secret Demo:** Runs out-of-the-box with a deterministic `MockMarketDataAdapter`.

## Quickstart

```bash
pip install -r requirements.txt
python demo_recorder.py
pytest tests/ -v
```
