from app.database import SessionLocal
from app.services.analytics import refresh_state_month_summary, refresh_sector_month_summary, get_price_movers

db = SessionLocal()

try:
    print("state summary rows written:", refresh_state_month_summary(db))
    print("sector summary rows written:", refresh_sector_month_summary(db))
    db.commit()

    print("price movers:", get_price_movers(db, "2024-01"))
finally:
    db.close()