from pydantic import BaseModel, field_validator
from datetime import datetime


class EnergyMessage(BaseModel):
    country_code: str
    bidding_zone: str
    timestamp: datetime
    period_end: datetime
    load_mw: float
    ingested_at: datetime
    source: str = "entsoe"
    resolution: str = "PT60M"

    @field_validator("load_mw")
    @classmethod
    def validate_load(cls, v):
        if v < 0:
            raise ValueError(f"load_mw {v} cannot be negative")
        if v > 150_000:
            raise ValueError(f"load_mw {v} unrealistically high")
        return v

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v):
        allowed = {"DE", "FR", "ES", "IT", "PL", "NL", "BE", "AT", "CH", "CZ"}
        if v not in allowed:
            raise ValueError(f"country_code {v} not in allowed set")
        return v