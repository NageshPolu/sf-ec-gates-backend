# main.py
from __future__ import annotations

import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, DateTime, JSON, String, Index
from sqlalchemy.orm import declarative_base, sessionmaker

from sf_client import SFClient, normalize_base_url
from gates import run_ec_gates


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

# Legacy table (your existing one)
class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    metrics = Column(JSON, nullable=False)

# NEW table (safe filtering by instance)
class SnapshotV2(Base):
    __tablename__ = "snapshots_v2"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # ✅ reliable filters
    instance_url = Column(String(300), nullable=False, index=True)
    api_base_url = Column(String(300), nullable=False, index=True)

    metrics = Column(JSON, nullable=False)

    __table_args__ = (
        Index("ix_snapshots_v2_instance_created", "instance_url", "created_at"),
    )

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

    if not instance_url:
        raise HTTPException(status_code=400, detail="Missing instance_url")
    if not api_base_url:
        # allow fallback to env SF_BASE_URL, but instance_url is still required
        api_base_url = normalize_base_url(DEFAULT_SF_BASE_URL)
        if not api_base_url:
            raise HTTPException(status_code=400, detail="Missing api_base_url and SF_BASE_URL is not set")

    sf = make_sf_client(api_base_url)

    try:
        # gates.py can optionally accept these; if not, it should ignore via **kwargs
        metrics = run_ec_gates(sf, instance_url=instance_url, api_base_url=api_base_url)
    except TypeError:
        # gates.py old signature
        metrics = run_ec_gates(sf)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Run failed: {str(e)}")

    # hard guard: never store empty metrics
    if not metrics or not metrics.get("snapshot_time_utc"):
        raise HTTPException(status_code=500, detail="Run produced empty metrics (guard blocked saving)")

    # ✅ force these keys into metrics so UI always shows correct source
    metrics["instance_url"] = instance_url
    metrics["api_base_url"] = api_base_url

    db = SessionLocal()
    try:
        s2 = SnapshotV2(instance_url=instance_url, api_base_url=api_base_url, metrics=metrics)
        db.add(s2)
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
        # Prefer v2 (reliable filtering)
        q2 = db.query(SnapshotV2)
        if instance_url:
            q2 = q2.filter(SnapshotV2.instance_url == instance_url)

        snap2 = q2.order_by(SnapshotV2.created_at.desc()).first()
        if snap2:
            return {"status": "ok", "metrics": snap2.metrics}

        # Fallback to legacy snapshots table (no instance filtering)
        snap = db.query(Snapshot).order_by(Snapshot.created_at.desc()).first()
        if not snap:
            return {"status": "empty"}

        return {"status": "ok", "metrics": snap.metrics}

    finally:
        db.close()
