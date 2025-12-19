# main.py
from __future__ import annotations

import os
import inspect
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker

from sf_client import SFClient, normalize_base_url
from gates import run_ec_gates


# -----------------------------
# Config
# -----------------------------
DB_URL = os.getenv("DATABASE_URL")
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

    # per-tenant creds (sent from Streamlit)
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


@app.get("/probe")
def probe(
    api_base_url: str = Query(...),
    username: str = Query(...),
    password: str = Query(...),
    timeout: int = Query(60),
    verify_ssl: bool = Query(True),
):
    """
    Quick sanity check: confirms the given api_base_url + creds return real OData JSON.
    Useful for Streamlit to validate derived/override API base.
    """
    sf = make_sf_client(api_base_url, username, password, timeout, verify_ssl)
    try:
        ok = sf.probe()
        return {"ok": bool(ok)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Probe failed: {str(e)}")


def _run_ec_gates_compat(sf: SFClient, *, instance_url: str, api_base_url: str, company_id: str | None):
    """
    Call run_ec_gates with company_id only if gates.py supports it.
    This prevents: 'unexpected keyword argument company_id'
    """
    sig = inspect.signature(run_ec_gates)
    kwargs = {"instance_url": instance_url, "api_base_url": api_base_url}

    if "company_id" in sig.parameters:
        kwargs["company_id"] = company_id

    return run_ec_gates(sf, **kwargs)


@app.post("/run")
def run_now(req: RunRequest):
    instance_url = normalize_base_url(req.instance_url or "")
    api_base_url = normalize_base_url(req.api_base_url or "")
    company_id = (req.company_id or "").strip() or None

    if not instance_url:
        raise HTTPException(status_code=400, detail="Missing instance_url")
    if not api_base_url:
        raise HTTPException(status_code=400, detail="Missing api_base_url")

    sf = make_sf_client(
        api_base_url=api_base_url,
        username=req.username or "",
        password=req.password or "",
        timeout=int(req.timeout or 60),
        verify_ssl=bool(True if req.verify_ssl is None else req.verify_ssl),
    )

    # Optional: force a probe first so wrong base URL shows a clean error early
    try:
        sf.probe()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid API base URL or credentials: {str(e)}")

    try:
        metrics = _run_ec_gates_compat(
            sf,
            instance_url=instance_url,
            api_base_url=api_base_url,
            company_id=company_id,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Run failed: {str(e)}")

    # Guard: never store an empty snapshot
    if not metrics or not metrics.get("snapshot_time_utc"):
        raise HTTPException(status_code=500, detail="Run produced empty metrics (guard blocked saving)")

    # Always persist scope fields (for correct filtering in Streamlit)
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
    """
    Latest snapshot for an instance; optionally scoped further by company_id.
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
