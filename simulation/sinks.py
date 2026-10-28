"""
Where stream messages go.

  EventHubSink  real Azure Event Hubs; one producer per hub, batches keyed by
                ride_id so a ride's messages stay in one partition, in order
  JsonlSink     dry run: one JSON line per message with the hub and send time,
                to test the live replay without Azure
"""

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class EventHubSink:

    def __init__(self, connections: Dict[str, str], retry_attempts: int = 3, backoff_s: float = 1.0,
                 producer_factory: Optional[Callable] = None, sleep: Callable[[float], None] = time.sleep):
        """
        connections: hub name -> connection string (namespace-level or hub-level).
        producer_factory is injectable for tests; defaults to the Azure SDK.
        """
        if producer_factory is None:
            from azure.eventhub import EventHubProducerClient
            producer_factory = lambda conn, hub: EventHubProducerClient.from_connection_string(conn, eventhub_name=hub)
        self.producers = {hub: producer_factory(conn, hub) for hub, conn in connections.items()}
        self.retry_attempts = retry_attempts
        self.backoff_s = backoff_s
        self.sleep = sleep
        self.sent = defaultdict(int)
        self.failed = defaultdict(int)

    def send(self, hub: str, messages: List[Dict], partition_key: str) -> bool:
        from azure.eventhub import EventData

        producer = self.producers[hub]
        for attempt in range(1, self.retry_attempts + 1):
            try:
                batch = producer.create_batch(partition_key=partition_key)
                for message in messages:
                    data = EventData(json.dumps(message))
                    try:
                        batch.add(data)
                    except ValueError:                     # batch full: send it, start another
                        producer.send_batch(batch)
                        batch = producer.create_batch(partition_key=partition_key)
                        batch.add(data)
                producer.send_batch(batch)
                self.sent[hub] += len(messages)
                return True
            except Exception as exc:                       # network / throttling
                logger.warning("Send to %s failed (attempt %d/%d): %s", hub, attempt, self.retry_attempts, exc)
                if attempt < self.retry_attempts:
                    self.sleep(self.backoff_s * 2 ** (attempt - 1))     # 1 s, 2 s, 4 s
        self.failed[hub] += len(messages)
        return False

    def close(self) -> None:
        for producer in self.producers.values():
            producer.close()


class JsonlSink:

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("w", encoding="utf8")
        self.sent = defaultdict(int)
        self.failed = defaultdict(int)

    def send(self, hub: str, messages: List[Dict], partition_key: str) -> bool:
        sent_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        for message in messages:
            self._file.write(json.dumps({"hub": hub, "sent_at": sent_at, "partition_key": partition_key,
                                         "body": message}) + "\n")
        self.sent[hub] += len(messages)
        return True

    def close(self) -> None:
        self._file.close()
