# main.py
from __future__ import annotations

import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker

from sf_client import SFClient, normalize_base_url
from gates import run_ec_gates


DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL is missing")


Base = declarative_base()

class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    metrics = Column(JSON, nullable=False)


engine = create_engine(DB_URL, pool_pre_ping=True)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

app = FastAPI()


class RunRequest(BaseModel):
    instance_url: str | None = None
    api_base_url: str | None = None

    # per-tenant creds
    username: str | None = None
    password: str | None = None
    company_id: str | None = None

    # runtime tuning
    timeout: int | None = 60
    verify_ssl: bool | None = True


@app.get("/health")
def health():
    return {"ok": True}


def make_sf_client(api_base_url: str, username: str, password: str, timeout: int, verify_ssl: bool) -> SFClient:
    base = normalize_base_url(api_base_url)
    if not base:
        raise HTTPException(status_code=400, detail="Missing api_base_url")

    u = (username or "").strip()
    p = password or ""
    if not u or not p:
        raise HTTPException(status_code=400, detail="Missing username or password")

    t = int(timeout or 60)
    v = bool(True if verify_ssl is None else verify_ssl)
    return SFClient(base, u, p, timeout=t, verify_ssl=v)


@app.post("/run")
def run_now(req: RunRequest):
    instance_url = normalize_base_url(req.instance_url or "")
    api_base_url = normalize_base_url(req.api_base_url or "")
    company_id = (req.company_id or "").strip() or None

    # Create client with per-tenant creds
    sf = make_sf_client(
        api_base_url=api_base_url,
        username=req.username or "",
        password=req.password or "",
        timeout=int(req.timeout or 60),
        verify_ssl=bool(True if req.verify_ssl is None else req.verify_ssl),
    )

    # Run gates (this MUST accept company_id in gates.py)
    try:
        metrics = run_ec_gates(sf, instance_url=instance_url, api_base_url=api_base_url, company_id=company_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Run failed: {str(e)}")

    # Guard: must have a real snapshot
    if not metrics or not metrics.get("snapshot_time_utc"):
        raise HTTPException(status_code=500, detail="Run produced empty metrics (guard blocked saving)")

    # Ensure scope fields exist (for filtering)
    metrics["instance_url"] = instance_url
    metrics["api_base_url"] = api_base_url
    metrics["company_id"] = company_id or ""

    db = SessionLocal()
    try:
        s = Snapshot(metrics=metrics)
        db.add(s)
        db.commit()
    finally:
        db.close()

    return {"ok": True, "metrics": metrics}


@app.get("/metrics/latest")
def latest_metrics(
    instance_url: str | None = Query(default=None),
    company_id: str | None = Query(default=None),
):
    instance_url = normalize_base_url(instance_url or "")
    company_id = (company_id or "").strip()

    db = SessionLocal()
    try:
        q = db.query(Snapshot)

        if instance_url:
            q = q.filter(Snapshot.metrics["instance_url"].as_string() == instance_url)

        if company_id:
            q = q.filter(Snapshot.metrics["company_id"].as_string() == company_id)

        snap = q.order_by(Snapshot.created_at.desc()).first()

        if not snap:
            return {"status": "empty"}

        return {"status": "ok", "metrics": snap.metrics}
    finally:
        db.close()
