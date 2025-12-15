import os
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

from sf_client import SFClient
from gates import run_ec_gates

app = FastAPI()

DB_URL = os.getenv("DATABASE_URL")
SF_BASE_URL = os.getenv("SF_BASE_URL")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")

Base = declarative_base()

class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    metrics = Column(JSON, nullable=False)

engine = create_engine(DB_URL, pool_pre_ping=True)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

def sf_client():
    return SFClient(SF_BASE_URL, SF_USERNAME, SF_PASSWORD)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run")
def run_now():
    sf = sf_client()
    metrics = run_ec_gates(sf)

    db = SessionLocal()
    try:
        db.add(Snapshot(metrics=metrics))
        db.commit()
    finally:
        db.close()

    return {"status": "ok", "snapshot_time_utc": metrics["snapshot_time_utc"]}

@app.get("/metrics/latest")
def latest():
    db = SessionLocal()
    try:
        snap = db.query(Snapshot).order_by(Snapshot.id.desc()).first()
        return {"status": "empty"} if not snap else {"status": "ok", "metrics": snap.metrics}
    finally:
        db.close()
