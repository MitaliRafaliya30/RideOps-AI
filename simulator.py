"""
RideOps AI Event Simulator
Main entry point: Generates rides in real-time and sends to Azure Event Hubs
"""

import sys
import time
import logging
from datetime import datetime
from random import Random

from dotenv import load_dotenv

from database.utils.database import PostgresConnection
from database.utils.config_loader import load_config, print_config_summary
from database.generators.entity_pool import EntityPool
from database.generators.ride_event_generator import RideEventGenerator
from database.generators.defect_injector import DefectInjector
from database.generators.event_hub_emitter import EventHubEmitter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RideopsSimulator:
    """
    Main simulator orchestrator
    
    Flow:
    1. Load config
    2. Initialize entity pool
    3. Generate rides continuously
    4. Inject defects
    5. Send to Event Hubs (REAL)
    """
    
    def __init__(self, config_path=None, cli_args=None):
        """
        Initialize simulator
        
        Args:
            config_path: Path to config file
            cli_args: CLI arguments
        """
        
        # Load config
        self.config = load_config(config_path, cli_args)
        print_config_summary(self.config)
        
        # Initialize PostgreSQL connection
        try:
            self.db_conn = PostgresConnection()
            logger.info("✓ Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
        
        # Initialize entity pool
        seed = self.config.get("simulator", {}).get("seed")
        try:
            self.entity_pool = EntityPool(self.db_conn, seed=seed)
            logger.info("✓ Loaded entity pool")
        except Exception as e:
            logger.error(f"Failed to load entity pool: {e}")
            raise
        
        # Initialize generators
        self.rng = Random(seed)
        self.generator = RideEventGenerator(self.entity_pool, self.config, self.rng)
        self.defect_injector = DefectInjector(self.config, Random(seed or 0))
        
        # Initialize Event Hubs emitter (REAL, not simulated)
        try:
            self.emitter = EventHubEmitter(self.config)
            logger.info("✓ Connected to Azure Event Hubs (REAL)")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hubs: {e}")
            raise
        
        # Simulator settings
        self.duration_seconds = self.config.get("simulator", {}).get("duration_seconds", 3600)
        self.rate_rides_per_second = self.config.get("simulator", {}).get("rate_rides_per_second", 10)
        
        # Metrics
        self.rides_generated = 0
        self.events_emitted = 0
        self.start_time = datetime.utcnow()
    
    def run(self):
        """
        Run simulator for configured duration
        
        Generates rides in real-time and sends to Event Hubs
        """
        
        logger.info(f"\n{'='*70}")
        logger.info(f"Starting RideOps AI Simulator")
        logger.info(f"Duration: {self.duration_seconds} seconds")
        logger.info(f"Rate: {self.rate_rides_per_second} rides/sec")
        logger.info(f"Defects: {'ENABLED' if self.config.get('defects', {}).get('enabled') else 'DISABLED'}")
        logger.info(f"{'='*70}\n")
        
        try:
            # Track actual start time
            simulation_start = time.time()
            ride_interval = 1.0 / self.rate_rides_per_second  # Seconds between rides
            next_ride_time = simulation_start  # Next ride generation time
            last_progress_time = simulation_start  # Last time we printed progress
            
            while True:
                current_time = time.time()
                elapsed = current_time - simulation_start
                
                # Stop if duration exceeded
                if elapsed >= self.duration_seconds:
                    logger.info(f"\n✓ Reached configured duration ({self.duration_seconds}s)")
                    break
                
                # Generate ride at configured rate
                if current_time >= next_ride_time:
                    self._generate_and_send_ride()
                    next_ride_time = current_time + ride_interval
                
                # Print metrics every 10 seconds
                if (current_time - last_progress_time) >= 10:
                    self._print_progress()
                    last_progress_time = current_time
                
                # Sleep briefly to prevent CPU spinning
                time.sleep(0.01)
            
            logger.info(f"\n{'='*70}")
            logger.info("Simulator completed successfully")
            self._print_final_metrics()
            logger.info(f"{'='*70}\n")
            
        except KeyboardInterrupt:
            logger.info("\n✓ Simulator stopped by user")
            self._print_final_metrics()
        except Exception as e:
            logger.error(f"\n✗ Simulator failed: {e}")
            raise   
    
    def _generate_and_send_ride(self):
        """
        Generate one ride, inject defects, and send to Event Hubs
        """

        try:
            # Generate clean ride
            now = datetime.utcnow()
            ride = self.generator.generate_ride(now)

            if not ride:
                return

            # Inject defects
            ride = self.defect_injector.inject(ride)

            # Send to Event Hubs (REAL)
            if self.emitter.send(ride):
                self.rides_generated += 1
                self.events_emitted += len(ride)

        except Exception as e:
            logger.error(f"Failed to generate/send ride: {e}")
    
    def _print_progress(self):
        """Print progress metrics"""
        
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        rides_per_sec = self.rides_generated / elapsed if elapsed > 0 else 0
        events_per_sec = self.events_emitted / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"Progress: {elapsed:.0f}s | "
            f"Rides: {self.rides_generated:,} ({rides_per_sec:.1f}/s) | "
            f"Events: {self.events_emitted:,} ({events_per_sec:.1f}/s)"
        )
    
    def _print_final_metrics(self):
        """Print final metrics"""
        
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        rides_per_sec = self.rides_generated / elapsed if elapsed > 0 else 0
        events_per_sec = self.events_emitted / elapsed if elapsed > 0 else 0
        
        eh_metrics = self.emitter.get_metrics()
        
        print("\n" + "=" * 70)
        print("Final Metrics")
        print("=" * 70)
        print(f"\nSimulator:")
        print(f"  Duration: {elapsed:.1f} seconds")
        print(f"  Rides generated: {self.rides_generated:,}")
        print(f"  Events generated: {self.events_emitted:,}")
        print(f"  Rides/sec: {rides_per_sec:.2f}")
        print(f"  Events/sec: {events_per_sec:.2f}")
        
        print(f"\nEvent Hubs (REAL):")
        print(f"  Events sent: {eh_metrics['events_sent']:,}")
        print(f"  Events failed: {eh_metrics['events_failed']:,}")
        print(f"  Bytes sent: {eh_metrics['bytes_sent']:,}")
        print(f"  Batches sent: {eh_metrics['batches_sent']:,}")
        
        print("=" * 70 + "\n")


def main():
    """Main entry point"""
    
    # Load environment
    load_dotenv()
    
    # Parse CLI arguments (if provided)
    cli_args = sys.argv[1:] if len(sys.argv) > 1 else None
    
    # Initialize and run simulator
    simulator = RideopsSimulator(cli_args=cli_args)
    simulator.run()


if __name__ == "__main__":
    main()