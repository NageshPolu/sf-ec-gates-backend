from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker

from sf_client import SFClient, normalize_base_url
from gates import run_ec_gates


DB_URL = os.getenv("DATABASE_URL")
DEFAULT_SF_BASE_URL = os.getenv("SF_BASE_URL", "")
SF_USERNAME = os.getenv("SF_USERNAME", "")
SF_PASSWORD = os.getenv("SF_PASSWORD", "")

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


@app.get("/health")
def health():
    return {"ok": True}


def _host(u: str) -> str:
    u = normalize_base_url(u or "")
    if not u:
        return ""
    if "://" not in u:
        u = "https://" + u
    return (urlparse(u).hostname or "").lower()


def derive_candidates(instance_url: str, api_base_url: str | None) -> list[str]:
    cand = []

    api_base_url = normalize_base_url(api_base_url or "")
    instance_url = normalize_base_url(instance_url or "")

    if api_base_url:
        cand.append(api_base_url)

    h = _host(instance_url)
    if h:
        # try instance host itself (often correct)
        cand.append("https://" + h)

        # try apiXX for hcmXX.sapsf.com
        if h.startswith("hcm") and h.endswith(".sapsf.com"):
            cand.append("https://" + h.replace("hcm", "api", 1))

    if DEFAULT_SF_BASE_URL:
        cand.append(normalize_base_url(DEFAULT_SF_BASE_URL))

    # de-dup while preserving order
    out = []
    for x in cand:
        x = normalize_base_url(x)
        if x and x not in out:
            out.append(x)
    return out


def make_sf_client(instance_url: str, api_base_url: str | None) -> SFClient:
    if not SF_USERNAME or not SF_PASSWORD:
        raise HTTPException(status_code=400, detail="Missing SF_USERNAME or SF_PASSWORD env vars")

    candidates = derive_candidates(instance_url, api_base_url)
    if not candidates:
        raise HTTPException(status_code=400, detail="No API base candidates available")

    last_err = None
    for base in candidates:
        try:
            c = SFClient(base, SF_USERNAME, SF_PASSWORD, timeout=60, verify_ssl=True)
            if c.probe():
                return c
        except Exception as e:
            last_err = str(e)

    raise HTTPException(status_code=400, detail=f"Could not validate API base URL candidates. Last error: {last_err}")


@app.post("/run")
def run_now(req: RunRequest):
    instance_url = normalize_base_url(req.instance_url or "")
    api_base_url = normalize_base_url(req.api_base_url or "")

    if not instance_url:
        raise HTTPException(status_code=400, detail="instance_url is required")

    sf = make_sf_client(instance_url=instance_url, api_base_url=api_base_url)

    try:
        metrics = run_ec_gates(sf, instance_url=instance_url, api_base_url=api_base_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Run failed: {str(e)}")

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
    instance_url = normalize_base_url(instance_url or "")

    db = SessionLocal()
    try:
        q = db.query(Snapshot)

        if instance_url:
            # Postgres JSON filter (works on most Render Postgres setups)
            try:
                q = q.filter(Snapshot.metrics["instance_url"].as_string() == instance_url)
            except Exception:
                # fallback: scan last N
                snaps = q.order_by(Snapshot.created_at.desc()).limit(200).all()
                for s in snaps:
                    m = s.metrics or {}
                    if normalize_base_url(m.get("instance_url", "")) == instance_url:
                        return {"status": "ok", "metrics": m}
                return {"status": "empty"}

        snap = q.order_by(Snapshot.created_at.desc()).first()
        if not snap:
            return {"status": "empty"}

        return {"status": "ok", "metrics": snap.metrics}
    finally:
        db.close()
