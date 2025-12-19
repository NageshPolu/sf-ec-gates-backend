# main.py
from __future__ import annotations

import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker

from sf_client import SFClient, normalize_base_url
from gates import run_ec_gates  # <- your updated gates.py


# -----------------------------
# Config
# -----------------------------
DB_URL = os.getenv("DATABASE_URL")
DEFAULT_SF_BASE_URL = os.getenv("SF_BASE_URL", "")
SF_USERNAME = os.getenv("SF_USERNAME", "")
SF_PASSWORD = os.getenv("SF_PASSWORD", "")

if not DB_URL:
    raise RuntimeError("DATABASE_URL is missing")


# -----------------------------
# DB
# -----------------------------
Base = declarative_base()

class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    metrics = Column(JSON, nullable=False)

engine = create_engine(DB_URL, pool_pre_ping=True)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)


# -----------------------------
# API
# -----------------------------
app = FastAPI()


class RunRequest(BaseModel):
    instance_url: str | None = None
    api_base_url: str | None = None


@app.get("/health")
def health():
    return {"ok": True}


def make_sf_client(api_base_url: str | None) -> SFClient:
    base = normalize_base_url(api_base_url or DEFAULT_SF_BASE_URL)
    if not base:
        raise HTTPException(status_code=400, detail="Missing api_base_url and SF_BASE_URL is not set")

    if not SF_USERNAME or not SF_PASSWORD:
        raise HTTPException(status_code=400, detail="Missing SF_USERNAME or SF_PASSWORD env vars")

    return SFClient(base, SF_USERNAME, SF_PASSWORD, timeout=60, verify_ssl=True)


@app.post("/run")
def run_now(req: RunRequest):
    """
    Run gates now and store snapshot.
    MUST fail if SF returns 4xx/5xx (so UI doesn’t show fake “Run completed”).
    """
    instance_url = normalize_base_url(req.instance_url or "")
    api_base_url = normalize_base_url(req.api_base_url or "")

    sf = make_sf_client(api_base_url)

    try:
        metrics = run_ec_gates(sf, instance_url=instance_url, api_base_url=api_base_url)
    except Exception as e:
        # Return 500 so Streamlit shows Run failed (no empty snapshot saved)
        raise HTTPException(status_code=500, detail=f"Run failed: {str(e)}")

    # hard guard: never store empty metrics
    if not metrics or not metrics.get("snapshot_time_utc"):
        raise HTTPException(status_code=500, detail="Run produced empty metrics (guard blocked saving)")

    db = SessionLocal()
    try:
        s = Snapshot(metrics=metrics)
        db.add(s)
        db.commit()
    finally:
        db.close()

    return {"ok": True, "metrics": metrics}


@app.get("/metrics/latest")
def latest_metrics(instance_url: str | None = Query(default=None)):
    """
    If instance_url provided -> return latest snapshot for that instance only.
    Else -> return latest snapshot overall.
    """
    instance_url = normalize_base_url(instance_url or "")

    db = SessionLocal()
    try:
        q = db.query(Snapshot)

        if instance_url:
            # JSON filter for Postgres (works with SQLAlchemy JSON)
            # This expression is compatible with many setups; if your DB differs, tell me DB type.
            q = q.filter(Snapshot.metrics["instance_url"].as_string() == instance_url)

        snap = q.order_by(Snapshot.created_at.desc()).first()

        if not snap:
            return {"status": "empty"}

        return {"status": "ok", "metrics": snap.metrics}

    finally:
        db.close()
