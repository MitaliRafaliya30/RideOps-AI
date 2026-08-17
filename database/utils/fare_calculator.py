"""
Fare calculation for RideOps AI Event Simulator

Calculates realistic NYC ride-share fares based on:
- Base fare
- Distance ($/mile)
- Time ($/minute)
- Surcharges (congestion, airport, etc.)
- Tips
- Driver pay (75% of base+tolls)
"""

from decimal import Decimal
from typing import Dict, Tuple


class FareCalculator:
    """Calculate ride fares and splits"""
    
    # ==================== FARE COMPONENTS ====================
    
    BASE_FARE = Decimal("2.50")
    DISTANCE_RATE = Decimal("2.50")        # $ per mile
    TIME_RATE = Decimal("0.45")            # $ per minute
    
    # Surcharges
    CONGESTION_SURCHARGE = Decimal("2.75")
    AIRPORT_FEE = Decimal("2.75")
    CBD_CONGESTION_FEE = Decimal("2.50")
    
    # Taxes & fees
    BCF_PERCENT = Decimal("0.01")          # Black Car Fund: 1%
    SALES_TAX_PERCENT = Decimal("0.08875") # NYC sales tax: 8.875%
    
    # Tips
    TIP_PERCENT_MIN = Decimal("0.10")      # Minimum 10%
    TIP_PERCENT_MAX = Decimal("0.25")      # Maximum 25%
    
    # Driver split
    DRIVER_SHARE_PERCENT = Decimal("0.75") # Driver gets 75%
    
    @staticmethod
    def calculate_fare(
        trip_miles: float,
        trip_seconds: int,
        has_congestion_surcharge: bool = False,
        is_airport: bool = False,
        has_cbd_congestion: bool = False,
        tip_percent: float = None,
        rng=None
    ) -> Dict[str, Decimal]:
        """
        Calculate complete fare breakdown
        
        Args:
            trip_miles: Distance in miles
            trip_seconds: Trip duration in seconds
            has_congestion_surcharge: Whether congestion surcharge applies
            is_airport: Whether airport fee applies
            has_cbd_congestion: Whether CBD congestion fee applies
            tip_percent: Tip percentage (0.10-0.25), random if None
        
        Returns:
            Dict with all fare components:
            {
                'base_passenger_fare': Decimal,
                'tolls': Decimal,
                'bcf': Decimal,
                'sales_tax': Decimal,
                'congestion_surcharge': Decimal,
                'airport_fee': Decimal,
                'cbd_congestion_fee': Decimal,
                'tips': Decimal,
                'driver_pay': Decimal,
                'total_fare': Decimal
            }
        """
        
        # Convert inputs
        trip_miles = Decimal(str(trip_miles))
        trip_minutes = Decimal(trip_seconds) / Decimal("60")
        
        # Base fare calculation
        distance_charge = trip_miles * FareCalculator.DISTANCE_RATE
        time_charge = trip_minutes * FareCalculator.TIME_RATE
        
        base_passenger_fare = (
            FareCalculator.BASE_FARE +
            distance_charge +
            time_charge
        )
        
        # Tolls (not in v1, always 0)
        tolls = Decimal("0.00")
        
        # Black Car Fund (1% of base fare)
        bcf = base_passenger_fare * FareCalculator.BCF_PERCENT
        
        # Sales tax (on base + distance + time)
        sales_tax = base_passenger_fare * FareCalculator.SALES_TAX_PERCENT
        
        # Surcharges
        congestion_surcharge = (
            FareCalculator.CONGESTION_SURCHARGE 
            if has_congestion_surcharge 
            else Decimal("0.00")
        )
        
        airport_fee = (
            FareCalculator.AIRPORT_FEE 
            if is_airport 
            else Decimal("0.00")
        )
        
        cbd_congestion_fee = (
            FareCalculator.CBD_CONGESTION_FEE 
            if has_cbd_congestion 
            else Decimal("0.00")
        )
        
        # Tips (if not provided, random 10-25% using provided RNG)
        if tip_percent is None:
            if rng is not None:
                tip_percent = rng.uniform(0.10, 0.25)
            else:
                tip_percent = 0.15  # Default to middle of range if no RNG provided

        tip_percent = Decimal(str(tip_percent))
        tips = base_passenger_fare * tip_percent
        
        # Driver pay (75% of base + tolls, NO surcharges)
        driver_pay = (base_passenger_fare + tolls) * FareCalculator.DRIVER_SHARE_PERCENT
        
        # Total fare (what passenger pays)
        total_fare = (
            base_passenger_fare +
            bcf +
            sales_tax +
            congestion_surcharge +
            airport_fee +
            cbd_congestion_fee +
            tips
        )
        
        return {
            "base_passenger_fare": base_passenger_fare,
            "tolls": tolls,
            "bcf": bcf,
            "sales_tax": sales_tax,
            "congestion_surcharge": congestion_surcharge,
            "airport_fee": airport_fee,
            "cbd_congestion_fee": cbd_congestion_fee,
            "tips": tips,
            "driver_pay": driver_pay,
            "total_fare": total_fare,
        }