# main.py
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, SecretStr

from sqlalchemy import create_engine, Column, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker

from sf_client import SFClient, normalize_base_url
from gates import run_ec_gates


# -----------------------------
# Config
# -----------------------------
DB_URL = os.getenv("DATABASE_URL")
DEFAULT_TIMEOUT = int(os.getenv("SF_TIMEOUT", "60"))
DEFAULT_VERIFY_SSL = os.getenv("SF_VERIFY_SSL", "true").lower() in ("1", "true", "yes")

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
    instance_url: str
    api_base_url: str

    # per-tenant credentials
    username: str
    password: SecretStr

    # optional scoping
    company_id: Optional[str] = None

    # optional network controls
    timeout: Optional[int] = None
    verify_ssl: Optional[bool] = None


@app.get("/health")
def health():
    return {"ok": True}


def make_sf_client(req: RunRequest) -> SFClient:
    base = normalize_base_url(req.api_base_url)
    if not base:
        raise HTTPException(status_code=400, detail="Missing api_base_url")

    username = (req.username or "").strip()
    password = req.password.get_secret_value() if req.password else ""

    if not username or not password:
        raise HTTPException(status_code=400, detail="Missing username/password")

    timeout = int(req.timeout or DEFAULT_TIMEOUT)
    verify_ssl = bool(DEFAULT_VERIFY_SSL if req.verify_ssl is None else req.verify_ssl)

    return SFClient(base, username, password, timeout=timeout, verify_ssl=verify_ssl)


@app.post("/run")
def run_now(req: RunRequest):
    """
    Run gates now and store snapshot.
    IMPORTANT: Do not store secrets in DB snapshots.
    """
    instance_url = normalize_base_url(req.instance_url)
    api_base_url = normalize_base_url(req.api_base_url)
    company_id = (req.company_id or "").strip() or None

    sf = make_sf_client(req)

    try:
        metrics = run_ec_gates(sf, instance_url=instance_url, api_base_url=api_base_url, company_id=company_id)
    except HTTPException:
        raise
    except Exception as e:
        # Don't leak secrets; keep message tight
        raise HTTPException(status_code=500, detail=f"Run failed: {str(e)}")

    # Hard guard: never store empty metrics
    if not metrics or not metrics.get("snapshot_time_utc"):
        raise HTTPException(status_code=500, detail="Run produced empty metrics (guard blocked saving)")

    # Safety: ensure secrets aren't present
    for k in ("username", "password", "SF_USERNAME", "SF_PASSWORD"):
        if k in metrics:
            metrics.pop(k, None)

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
    """
    Latest snapshot overall, or filtered by instance_url (+ optionally company_id).
    """
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
