from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Tuple


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _try_get_all(sf, path: str, params_list: List[Dict[str, Any]]) -> Tuple[List[dict], str]:
    """
    Try multiple query variants (for tenants where some fields/nav aren't allowed).
    Returns (rows, strategy_label).
    """
    last_err = None
    for p in params_list:
        try:
            rows = sf.get_all(path, p)
            return rows, f"{path} params={p}"
        except Exception as e:
            last_err = str(e)
            continue
    raise RuntimeError(f"All query variants failed for {path}. Last error: {last_err}")


def _build_status_map(sf) -> Dict[str, str]:
    """
    Best-effort externalCode -> name mapping for employment status.
    Different tenants expose different entities; try a few common ones.
    """
    candidates = [
        ("/odata/v2/FOEmploymentStatus", {"$select": "externalCode,name", "$top": 1000}),
        ("/odata/v2/EmploymentStatus", {"$select": "externalCode,name", "$top": 1000}),
        ("/odata/v2/FOEmpEmploymentStatus", {"$select": "externalCode,name", "$top": 1000}),
    ]

    for path, params in candidates:
        try:
            rows = sf.get_all(path, params)
            m = {}
            for r in rows:
                code = str(r.get("externalCode") or "").strip()
                name = str(r.get("name") or "").strip()
                if code and name:
                    m[code] = name
            if m:
                return m
        except Exception:
            continue

    return {}


def run_ec_gates(sf, instance_url: str = "", api_base_url: str = "") -> dict:
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # USERS (for active/inactive + email hygiene)
    # ---------------------------
    users = sf.get_all(
        "/odata/v2/User",
        {"$select": "userId,status,email,username", "$top": 1000},
    )

    def is_active_user(u: dict) -> bool:
        s = str(u.get("status", "")).strip().lower()
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user(u)]
    inactive_users = [u for u in users if not is_active_user(u)]

    total_active = len(active_users)
    inactive_user_count = len(inactive_users)

    inactive_users_sample = [
        {"userId": u.get("userId"), "status": u.get("status"), "email": u.get("email"), "username": u.get("username")}
        for u in inactive_users[:MAX_SAMPLE]
    ]

    missing_email_sample = []
    email_to_users = defaultdict(list)

    for u in active_users:
        uid = u.get("userId")
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        email_norm = email.lower()

        if is_missing_email_value(email):
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append({"userId": uid, "email": email, "username": u.get("username")})
            continue

        email_to_users[email_norm].append(uid)

    missing_email_count = sum(1 for u in active_users if is_missing_email_value(u.get("email")))

    duplicate_email_count = sum((len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1)

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append({"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]})
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # EMPJOB (robust fetch)
    # ---------------------------
    org_fields = ["company", "businessUnit", "division", "department", "location"]

    # Try with status + optional class fields, then strip down if tenant blocks fields
    params_variants = [
        {"$select": "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange",
         "$filter": "effectiveLatestChange eq true"},
        {"$select": "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange",
         "$filter": "effectiveLatestChange eq true", "$expand": "emplStatusNav"},
        # fallback if filter is blocked
        {"$select": "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange"},
        # fallback if emplStatus is blocked
        {"$select": "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange",
         "$filter": "effectiveLatestChange eq true"},
    ]

    jobs, empjob_fetch_strategy = _try_get_all(sf, "/odata/v2/EmpJob", params_variants)

    # Guard: if EmpJob returns 0, fail the run (so you don’t store fake zero snapshots)
    if len(jobs) == 0:
        raise RuntimeError(
            "EmpJob returned 0 rows. Check API base URL (api*.domain), credentials, and OData permissions for EmpJob."
        )

    # ---------------------------
    # Employee status mapping (Name (Code))
    # ---------------------------
    status_map = _build_status_map(sf)

    def fmt_status(code: Any) -> str:
        c = "" if code is None else str(code).strip()
        if not c:
            return ""
        n = status_map.get(c)
        return f"{n} ({c})" if n else f"(code {c})"

    # Build breakdown
    status_counts = defaultdict(int)
    for j in jobs:
        code = j.get("emplStatus")
        if code is not None:
            status_counts[str(code).strip()] += 1

    breakdown = []
    for code, cnt in sorted(status_counts.items(), key=lambda x: x[1], reverse=True)[:MAX_SAMPLE]:
        breakdown.append({"emplStatusCode": code, "emplStatusName": status_map.get(code), "display": fmt_status(code), "count": cnt})

    # ---------------------------
    # Org + manager checks
    # ---------------------------
    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in org_fields}

    for j in jobs:
        uid = j.get("userId")
        mgr = j.get("managerId")

        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append({"userId": uid, "managerId": mgr})

        missing_fields = [k for k in org_fields if is_blank(j.get(k))]
        if missing_fields:
            invalid_org_count += 1
            for f in missing_fields:
                org_missing_field_counts[f] += 1
            if len(invalid_org_sample) < MAX_SAMPLE:
                invalid_org_sample.append(
                    {
                        "userId": uid,
                        "missingFields": ", ".join(missing_fields),
                        "company": j.get("company"),
                        "businessUnit": j.get("businessUnit"),
                        "division": j.get("division"),
                        "department": j.get("department"),
                        "location": j.get("location"),
                        "managerId": mgr,
                    }
                )

    # ---------------------------
    # Contingent workers (prefer EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_worker_count = 0
    contingent_workers_sample = []
    contingent_source = "unknown"

    try:
        cont = sf.get_all(
            "/odata/v2/EmpEmployment",
            {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        )
        contingent_worker_count = len(cont)
        contingent_workers_sample = [{"userId": r.get("userId"), "isContingentWorker": r.get("isContingentWorker")} for r in cont[:MAX_SAMPLE]]
        contingent_source = "EmpEmployment.isContingentWorker"
    except Exception:
        contingent_source = "not-available (no EmpEmployment/isContingentWorker)"

    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    metrics = {
        "snapshot_time_utc": now.isoformat(),
        "instance_url": instance_url,
        "api_base_url_requested": api_base_url,
        "api_base_url_effective": getattr(sf, "base_url", ""),

        "active_users": total_active,
        "inactive_users": inactive_user_count,
        "inactive_user_count": inactive_user_count,

        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),
        "empjob_fetch_strategy": empjob_fetch_strategy,

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "missing_email_pct": missing_email_pct,

        "duplicate_email_count": duplicate_email_count,
        "risk_score": risk_score,

        "contingent_workers": contingent_worker_count,
        "contingent_worker_count": contingent_worker_count,
        "contingent_source": contingent_source,

        "employee_status_source": "EmpJob.emplStatus + FOEmploymentStatus(best-effort)" if status_map else "EmpJob.emplStatus (codes only; mapping not accessible)",
        "employee_status_breakdown": breakdown,

        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }

    return metrics
