from pydantic import BaseModel, field_validator
from datetime import datetime


class WeatherMessage(BaseModel):
    city_code: str
    city_name: str
    latitude: float
    longitude: float
    timestamp: datetime
    temperature_c: float
    relative_humidity_pct: float
    wind_speed_kmh: float
    solar_radiation_wm2: float
    precipitation_mm: float
    weather_code: int
    ingested_at: datetime
    source: str = "open-meteo"

    @field_validator("temperature_c")
    @classmethod
    def validate_temperature(cls, v):
        if not -90 <= v <= 60:
            raise ValueError(f"temperature_c {v} out of realistic range")
        return v

    @field_validator("relative_humidity_pct")
    @classmethod
    def validate_humidity(cls, v):
        if not 0 <= v <= 100:
            raise ValueError(f"relative_humidity_pct {v} must be 0-100")
        return v

    @field_validator("city_code")
    @classmethod
    def validate_city_code(cls, v):
        allowed = {"DE", "FR", "ES", "IT", "PL", "NL", "BE", "AT", "CH", "CZ"}
        if v not in allowed:
            raise ValueError(f"city_code {v} not in allowed set")
        return v