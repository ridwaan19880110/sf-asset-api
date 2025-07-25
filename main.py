from fastapi import FastAPI, Depends, Header, HTTPException, Path, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, aliased
from datetime import datetime
import secrets

# ---------------- Database Setup ---------------- #

DATABASE_URL = "mysql+pymysql://remote_root:koK3MUkW186JlS@72.18.214.201:3306/mnc_report"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# ---------------- FastAPI App ---------------- #

app = FastAPI()

# ---------------- Models ---------------- #

class CallReport(Base):
    __tablename__ = "simplyfleet"

    asset_id = Column(String(50), primary_key=True, index=True)
    asset_status = Column(String(50))
    last_movement_reason = Column(String(255))
    registration_number = Column(String(50))
    vin_number = Column(String(50))
    asset_type = Column(String(100))
    engine_number = Column(String(100))
    asset_make = Column(String(100))
    asset_model = Column(String(100))
    current_odo = Column(Integer)
    linked_driver_id = Column(String(50))
    modified_time = Column(DateTime)
    updatedAt = Column(DateTime)

class SimplyfleetDriver(Base):
    __tablename__ = "simplyfleet_driver"

    id = Column(String(50), primary_key=True, index=True)
    full_name = Column(String(100))
    whatsapp_number = Column(String(50))
    email = Column(String(100))

class APIKey(Base):
    __tablename__ = "api_keys"

    key = Column(String(64), primary_key=True, index=True)
    owner = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# ---------------- Schemas ---------------- #

class CallReportResponse(BaseModel):
    asset_id: str
    asset_status: Optional[str]
    last_movement_reason: Optional[str]
    registration_number: Optional[str]
    vin_number: Optional[str]
    asset_type: Optional[str]
    engine_number: Optional[str]
    asset_make: Optional[str]
    asset_model: Optional[str]
    current_odo: Optional[int]
    modified_time: Optional[datetime]
    updatedAt: Optional[datetime]
    full_name: Optional[str]
    phone: Optional[str]
    email: Optional[str]

    class Config:
        orm_mode = True

class CallReportUpdate(BaseModel):
    asset_status: Optional[str]
    last_movement_reason: Optional[str]
    modified_time: Optional[datetime] = datetime.utcnow()

# ---------------- Dependencies ---------------- #

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def validate_api_key(x_api_key: str = Header(...), db: Session = Depends(get_db)):
    key_obj = db.query(APIKey).filter(APIKey.key == x_api_key).first()
    if not key_obj:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return key_obj

# ---------------- Endpoints ---------------- #

@app.get("/")
def root():
    return {"message": "SF Asset API is running."}

@app.get("/assets", response_model=List[CallReportResponse])
def get_active_toyota_assets(db: Session = Depends(get_db), _: APIKey = Depends(validate_api_key)):
    Driver = aliased(SimplyfleetDriver)
    query = (
        db.query(
            CallReport.asset_id,
            CallReport.asset_status,
            CallReport.last_movement_reason,
            CallReport.registration_number,
            CallReport.vin_number,
            CallReport.asset_type,
            CallReport.engine_number,
            CallReport.asset_make,
            CallReport.asset_model,
            CallReport.current_odo,
            CallReport.modified_time,
            CallReport.updatedAt,
            Driver.full_name,
            Driver.whatsapp_number.label("phone"),
            Driver.email,
        )
        .outerjoin(Driver, Driver.id == CallReport.linked_driver_id)
        .filter(CallReport.asset_make == "Toyota")
        .filter(CallReport.asset_status == "Active")
    )
    return query.all()

@app.put("/assets/{asset_id}", response_model=CallReportResponse)
def update_asset(asset_id: str, payload: CallReportUpdate, db: Session = Depends(get_db), _: APIKey = Depends(validate_api_key)):
    asset = db.query(CallReport).filter(CallReport.asset_id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    for key, value in payload.dict(exclude_unset=True).items():
        setattr(asset, key, value)

    asset.updatedAt = datetime.utcnow()
    db.commit()
    db.refresh(asset)
    return asset

@app.put("/assets/odo/{registration_number}", response_model=CallReportResponse)
def update_current_odo(
    registration_number: str,
    current_odo: int = Query(..., description="New ODO reading"),
    db: Session = Depends(get_db),
    _: APIKey = Depends(validate_api_key)
):
    asset = db.query(CallReport).filter(CallReport.registration_number == registration_number).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    asset.current_odo = current_odo
    asset.updatedAt = datetime.utcnow()
    db.commit()
    db.refresh(asset)

    driver = db.query(SimplyfleetDriver).filter(SimplyfleetDriver.id == asset.linked_driver_id).first()

    return CallReportResponse(
        asset_id=asset.asset_id,
        asset_status=asset.asset_status,
        last_movement_reason=asset.last_movement_reason,
        registration_number=asset.registration_number,
        vin_number=asset.vin_number,
        asset_type=asset.asset_type,
        engine_number=asset.engine_number,
        asset_make=asset.asset_make,
        asset_model=asset.asset_model,
        current_odo=asset.current_odo,
        modified_time=asset.modified_time,
        updatedAt=asset.updatedAt,
        full_name=driver.full_name if driver else None,
        phone=driver.whatsapp_number if driver else None,
        email=driver.email if driver else None,
    )

@app.post("/generate-key")
def generate_key(owner: str, db: Session = Depends(get_db)):
    new_key = secrets.token_hex(32)
    db.add(APIKey(key=new_key, owner=owner))
    db.commit()
    return {"api_key": new_key}
