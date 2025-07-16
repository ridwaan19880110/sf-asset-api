from fastapi import FastAPI, Depends, Header, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
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
    asset_shape = Column(String(50))
    modified_time = Column(DateTime)
    updatedAt = Column(DateTime)

class APIKey(Base):
    __tablename__ = "api_keys"

    key = Column(String(64), primary_key=True, index=True)
    owner = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)

# Ensure tables exist
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
    asset_shape: Optional[str]
    modified_time: Optional[datetime]
    updatedAt: Optional[datetime]

    model_config = {
        "from_attributes": True
    }

class CallReportResponse(BaseModel):
    asset_id: str
    asset_status: Optional[str]
    last_movement_reason: Optional[str]
    registration_number: Optional[str]

    class Config:
        orm_mode = True

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
def get_assets(db: Session = Depends(get_db), _: APIKey = Depends(validate_api_key)):
    return db.query(CallReport).all()

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

@app.post("/generate-key")
def generate_key(owner: str, db: Session = Depends(get_db)):
    new_key = secrets.token_hex(32)
    db.add(APIKey(key=new_key, owner=owner))
    db.commit()
    return {"api_key": new_key}
