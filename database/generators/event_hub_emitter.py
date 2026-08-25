"""
Event Hub Emitter for RideOps AI Event Simulator
REAL Azure Event Hubs Integration (NOT simulated)
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from decimal import Decimal

from azure.eventhub import EventHubProducerClient, EventData

logger = logging.getLogger(__name__)


class EventHubEmitter:
    """
    Send ride events to REAL Azure Event Hubs
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize real Event Hubs emitter
        
        Args:
            config: Configuration dict with event_hubs settings
        """
        self.config = config
        
        # Extract Event Hubs config
        eh_cfg = config.get("event_hubs", {})
        self.connection_string = eh_cfg.get("connection_string")
        self.hub_name = eh_cfg.get("hub_name", "ride-events")
        self.batch_size = eh_cfg.get("emission", {}).get("batch_size", 1)
        
        # Initialize real producer
        try:
            self.producer = EventHubProducerClient.from_connection_string(
                self.connection_string,
                eventhub_name=self.hub_name
            )
            logger.info(f"✓ Connected to Event Hubs: {self.hub_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hubs: {e}")
            raise
        
        # Metrics
        self.events_sent = 0
        self.events_failed = 0
        self.bytes_sent = 0
        self.batches_sent = 0
        self.start_time = datetime.utcnow()
    
    def send(self, ride: List[Dict[str, Any]]) -> bool:
        """
        Send a ride's events to REAL Event Hubs
        
        Args:
            ride: List of event dicts
        
        Returns:
            True if successful
        """
        
        if not ride:
            return True
        
        try:
            # Serialize events
            serialized = [self._serialize_event(e) for e in ride]
            
            # Create batch
            event_data_batch = self.producer.create_batch()
            
            for event_json in serialized:
                event_data_batch.add(EventData(event_json))
            
            # Send to REAL Event Hubs
            self.producer.send_batch(event_data_batch)
            
            # Update metrics
            self.events_sent += len(ride)
            self.bytes_sent += sum(len(e.encode('utf-8')) for e in serialized)
            self.batches_sent += 1
            
            logger.debug(f"✓ Sent {len(ride)} events to Event Hubs")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send to Event Hubs: {e}")
            self.events_failed += len(ride)
            return False
    
    def _serialize_event(self, event: Dict[str, Any]) -> str:
        """Serialize event to JSON"""
        
        event_copy = dict(event)
        
        # Convert Decimal to string
        for key in event_copy:
            if isinstance(event_copy[key], Decimal):
                event_copy[key] = str(event_copy[key])
        
        return json.dumps(event_copy)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get metrics"""
        
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        
        return {
            "events_sent": self.events_sent,
            "events_failed": self.events_failed,
            "bytes_sent": self.bytes_sent,
            "batches_sent": self.batches_sent,
            "events_per_second": self.events_sent / elapsed if elapsed > 0 else 0,
        }
    
    def close(self) -> None:
        """Close connection"""
        try:
            self.producer.close()
            logger.info("✓ Event Hubs connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")