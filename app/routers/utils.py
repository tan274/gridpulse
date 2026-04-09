from fastapi import HTTPException


def validate_period(period: str) -> str:
    """Validate a YYYY-MM period string. Raises 400 if invalid."""
    try:
        parts = period.split("-")
        if len(parts) != 2:
            raise ValueError
        year, month = int(parts[0]), int(parts[1])
        if not (1 <= month <= 12):
            raise ValueError
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period '{period}'. Expected format: YYYY-MM (e.g. 2024-01)",
        )
    return period
