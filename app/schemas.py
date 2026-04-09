from typing import Literal

from pydantic import BaseModel, model_validator


class IngestRunRequest(BaseModel):
    mode: Literal["latest", "backfill"]
    start_period: str | None = None
    end_period: str | None = None
    state_ids: list[str] | None = None
    sector_ids: list[str] | None = None

    @model_validator(mode="after")
    def check_backfill_periods(self):
        if self.mode == "backfill":
            if not self.start_period or not self.end_period:
                raise ValueError("backfill mode requires both start_period and end_period")
        return self
