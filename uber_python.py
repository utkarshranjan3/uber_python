from abc import ABC, abstractmethod
from collections import defaultdict, deque, Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import reduce, lru_cache
import itertools
import logging

# Configure pipeline logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("UberPipeline")


# ==========================================
# 1. DATA MODELS & ENCAPSULATION (OOP)
# ==========================================

@dataclass(frozen=True)
class Location:
    """Immutable spatial coordinate representation."""
    lat: float
    lon: float
    zone_id: int


@dataclass
class TripRecord:
    """Encapsulates a single Uber trip event."""
    trip_id: str
    rider_id: str
    driver_id: str
    pickup_loc: Location
    dropoff_loc: Location
    base_fare: float
    distance_km: float
    timestamp: datetime
    status: str = "COMPLETED"


# ==========================================
# 2. STRATEGY PATTERN & POLYMORPHISM (OOP)
# ==========================================

class FareCalculator(ABC):
    """Abstract Base Class for dynamic pricing strategies."""
    
    @abstractmethod
    def calculate(self, base_fare: float, distance_km: float) -> float:
        pass


class StandardPricing(FareCalculator):
    def calculate(self, base_fare: float, distance_km: float) -> float:
        return round(base_fare + (distance_km * 1.5), 2)


class SurgePricing(FareCalculator):
    def __init__(self, multiplier: float = 1.8):
        self.multiplier = multiplier

    def calculate(self, base_fare: float, distance_km: float) -> float:
        return round((base_fare + (distance_km * 2.0)) * self.multiplier, 2)


# ==========================================
# 3. PIPELINE ENGINE & DATA STRUCTURES
# ==========================================

class UberAnalyticsEngine:
    """
    Main pipeline orchestrator utilizing advanced data structures 
    and functional programming constructs.
    """
    
    def __init__(self):
        # O(1) lookups for active sessions and spatial zones
        self.active_riders: set = set()
        self.zone_trip_counts: defaultdict = defaultdict(int)
        
        # Sliding window buffer using deque for real-time telemetry (max 5 items)
        self.recent_events_window: deque = deque(maxlen=5)
        
        # Internal storage for batch analytics
        self.trips: list[TripRecord] = []

    def ingest_trip(self, trip: TripRecord, pricing_strategy: FareCalculator) -> None:
        """Ingests trip data, applies pricing policy, and updates state structures."""
        logger.info(f"Ingesting trip {trip.trip_id} for rider {trip.rider_id}")
        
        # Track unique active users using a Set (O(1) average time complexity)
        self.active_riders.add(trip.rider_id)
        
        # Update spatial metrics using DefaultDict
        self.zone_trip_counts[trip.pickup_loc.zone_id] += 1
        
        # Calculate dynamic fare using polymorphism
        final_fare = pricing_strategy.calculate(trip.base_fare, trip.distance_km)
        
        # Push to sliding window buffer (Deque)
        self.recent_events_window.append({
            "trip_id": trip.trip_id,
            "fare": final_fare,
            "time": trip.timestamp.strftime("%H:%M:%S")
        })
        
        self.trips.append(trip)

    @lru_cache(maxsize=128)
    def compute_zone_metrics(self, zone_id: int) -> dict:
        """Demonstrates memoization for expensive spatial lookups."""
        zone_trips = [t for t in self.trips if t.pickup_loc.zone_id == zone_id]
        if not zone_trips:
            return {"total_trips": 0, "avg_distance": 0.0}
        
        total_dist = sum(t.distance_km for t in zone_trips)
        return {
            "total_trips": len(zone_trips),
            "avg_distance": round(total_dist / len(zone_trips), 2)
        }

    def generate_revenue_summary(self) -> float:
        """Uses functools.reduce and itertools for functional data aggregation."""
        fares = [t.base_fare + (t.distance_km * 1.5) for t in self.trips]
        
        # Functional reduction to calculate total revenue
        total_revenue = reduce(lambda acc, val: acc + val, fares, 0.0)
        return round(total_revenue, 2)

    def get_top_drivers(self, n: int = 2) -> list:
        """Uses collections.Counter for frequency counting."""
        driver_counter = Counter(t.driver_id for t in self.trips)
        return driver_counter.most_common(n)


# ==========================================
# 4. EXECUTION SIMULATION (Databricks Notebook Entrypoint)
# ==========================================

if __name__ == "__main__":
    logger.info("Initializing Uber Analytics Pipeline...")
    pipeline = UberAnalyticsEngine()

    # Mock Data Generation
    base_time = datetime.now()
    loc_a = Location(lat=12.9716, lon=77.5946, zone_id=101) # e.g., Indiranagar
    loc_b = Location(lat=12.9352, lon=77.6245, zone_id=102) # e.g., Koramangala

    trip1 = TripRecord("T-001", "R_101", "D_501", loc_a, loc_b, 50.0, 8.5, base_time)
    trip2 = TripRecord("T-002", "R_102", "D_502", loc_b, loc_a, 40.0, 5.2, base_time + timedelta(minutes=10))
    trip3 = TripRecord("T-003", "R_101", "D_501", loc_a, loc_b, 60.0, 10.1, base_time + timedelta(minutes=20))

    # Ingest using different pricing strategies (Polymorphism in action)
    standard_strat = StandardPricing()
    surge_strat = SurgePricing(multiplier=2.0)

    pipeline.ingest_trip(trip1, standard_strat)
    pipeline.ingest_trip(trip2, standard_strat)
    pipeline.ingest_trip(trip3, surge_strat)

    # Output Analytics
    logger.info(f"Total Unique Active Riders: {len(pipeline.active_riders)}")
    logger.info(f"Zone 101 Metrics: {pipeline.compute_zone_metrics(101)}")
    logger.info(f"Total Projected Revenue: ${pipeline.generate_revenue_summary()}")
    logger.info(f"Top Drivers: {pipeline.get_top_drivers(1)}")
    logger.info(f"Sliding Window Recent Events: {list(pipeline.recent_events_window)}")
