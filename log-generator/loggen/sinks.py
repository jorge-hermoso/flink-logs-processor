"""Where the generated lines go: standard output, a file, or a Kinesis data stream."""

from __future__ import annotations

import sys
import time
from typing import Dict, List


class StreamSink:
    """Writes one line per log line to a text stream."""

    def __init__(self, stream=None, close=False):
        self._stream = stream or sys.stdout
        self._close = close

    @classmethod
    def to_file(cls, path: str) -> "StreamSink":
        return cls(open(path, "a", encoding="utf-8", errors="replace"), close=True)

    def write(self, source: str, line: str) -> None:
        self._stream.write(line + "\n")

    def flush(self) -> None:
        self._stream.flush()

    def close(self) -> None:
        self.flush()
        if self._close:
            self._stream.close()


class KinesisSink:
    """Batches lines into PutRecords calls.

    * The partition key is the line's source, so one station's lines stay ordered on one shard.
    * ``lines_per_record`` > 1 packs several newline-separated lines of the same source into one
      record, the way log shipping agents do.
    * PutRecords can partially fail (throttling): only the failed records are retried, with
      exponential backoff. After ``max_attempts`` the sink raises rather than silently drop logs.
    """

    MAX_BATCH = 500  # PutRecords limit

    def __init__(self, client, stream_name: str, lines_per_record: int = 1, max_attempts: int = 8,
                 flush_interval_s: float = 1.0, sleep=time.sleep, clock=time.monotonic):
        self._client = client
        self._stream_name = stream_name
        self._lines_per_record = max(1, lines_per_record)
        self._max_attempts = max_attempts
        self._flush_interval_s = flush_interval_s
        self._sleep = sleep
        self._clock = clock
        self._partial: Dict[str, List[str]] = {}
        self._records: List[dict] = []
        self._last_flush = clock()
        self.records_sent = 0

    def write(self, source: str, line: str) -> None:
        lines = self._partial.setdefault(source, [])
        lines.append(line)
        if len(lines) >= self._lines_per_record:
            self._seal(source)
        if len(self._records) >= self.MAX_BATCH or self._clock() - self._last_flush >= self._flush_interval_s:
            self.flush()

    def _seal(self, source: str) -> None:
        lines = self._partial.pop(source, None)
        if lines:
            data = "\n".join(lines).encode("utf-8", errors="replace")
            self._records.append({"Data": data, "PartitionKey": source})

    def flush(self) -> None:
        for source in list(self._partial):
            self._seal(source)
        while self._records:
            batch, self._records = self._records[:self.MAX_BATCH], self._records[self.MAX_BATCH:]
            self._put(batch)
        self._last_flush = self._clock()

    def _put(self, batch: List[dict]) -> None:
        for attempt in range(1, self._max_attempts + 1):
            response = self._client.put_records(StreamName=self._stream_name, Records=batch)
            if not response.get("FailedRecordCount"):
                self.records_sent += len(batch)
                return
            failed = [record for record, result in zip(batch, response["Records"]) if "ErrorCode" in result]
            self.records_sent += len(batch) - len(failed)
            batch = failed
            if attempt < self._max_attempts:
                self._sleep(min(10.0, 0.1 * 2 ** attempt))
        raise RuntimeError(f"{len(batch)} records still rejected by stream '{self._stream_name}' "
                           f"after {self._max_attempts} attempts")

    def close(self) -> None:
        self.flush()


def kinesis_client(region: str = None, endpoint_url: str = None):
    """Kinesis client with adaptive retries. boto3 is only needed for this sink."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError as error:  # pragma: no cover - depends on the environment
        raise SystemExit("The kinesis sink needs boto3: pip install 'zorblatt-loggen[kinesis]'") from error
    return boto3.client("kinesis", region_name=region, endpoint_url=endpoint_url,
                        config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))
