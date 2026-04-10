from typing import Literal

from pydantic import BaseModel, field_validator, model_validator


class IngestRunRequest(BaseModel):
    mode: Literal["latest", "backfill"]
    start_period: str | None = None
    end_period: str | None = None
    state_ids: list[str] | None = None
    sector_ids: list[str] | None = None

    @field_validator("start_period", "end_period", mode="before")
    @classmethod
    def validate_period_format(cls, v):
        if v is None:
            return v
        try:
            parts = v.split("-")
            if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
                raise ValueError
            year, month = int(parts[0]), int(parts[1])
            if not (1 <= month <= 12):
                raise ValueError
        except (ValueError, AttributeError):
            raise ValueError(f"Invalid period '{v}'. Expected YYYY-MM (e.g. 2024-01)")
        return v

    @model_validator(mode="after")
    def check_backfill_periods(self):
        if self.mode == "backfill":
            if not self.start_period or not self.end_period:
                raise ValueError("backfill mode requires both start_period and end_period")
            if self.start_period > self.end_period:
                raise ValueError("start_period must be <= end_period")
        return self
