# gates.py
from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional

# IMPORTANT:
# Do NOT import run_ec_gates from gates (self-import). That causes circular import.
# main.py imports run_ec_gates from here.


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def safe_get_all(sf, path: str, params: dict) -> List[dict]:
    """
    Wrapper to call sf.get_all but raise clean error messages.
    """
    try:
        return sf.get_all(path, params)
    except Exception as e:
        raise RuntimeError(f"SF API error calling {path} with params={params}: {e}")


def run_ec_gates(
    sf,
    *,
    instance_url: str = "",
    api_base_url: str = "",
    company_id: Optional[str] = None,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # USERS (Active/Inactive + Email Hygiene)
    # ---------------------------
    users = safe_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
    )

    def is_active_user(u: dict) -> bool:
        s = str(u.get("status", "")).strip().lower()
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user(u)]
    inactive_users = [u for u in users if not is_active_user(u)]

    total_active = len(active_users)
    inactive_user_count = len(inactive_users)

    inactive_users_sample = [
        {
            "userId": u.get("userId"),
            "status": u.get("status"),
            "email": u.get("email"),
            "username": u.get("username"),
        }
        for u in inactive_users[:MAX_SAMPLE]
    ]

    # Missing email + duplicates among ACTIVE users
    missing_email_sample = []
    email_to_users = defaultdict(list)

    for u in active_users:
        uid = u.get("userId")
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        email_norm = email.lower()

        if is_missing_email_value(email):
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append(
                    {"userId": uid, "email": email, "username": u.get("username")}
                )
            continue

        email_to_users[email_norm].append(uid)

    missing_email_count = sum(1 for u in active_users if is_missing_email_value(u.get("email")))

    duplicate_email_count = sum((len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1)

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append(
                {"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]}
            )
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # EMPJOB (Latest records) with safe fallback
    # ---------------------------
    # Some tenants throw 400 if you request a field that doesn't exist.
    # We'll try progressively smaller selects until it works.
    empjob_select_candidates = [
        # Rich (for contingent detection + status)
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange,employeeClass,employeeType,employmentType",
        # Without employeeType/employmentType
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange,employeeClass",
        # Without emplStatus
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange,employeeClass,employeeType,employmentType",
        # Minimal
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange",
    ]

    jobs: List[dict] = []
    used_select = None
    last_err = None
    for sel in empjob_select_candidates:
        try:
            jobs = safe_get_all(
                sf,
                "/odata/v2/EmpJob",
                {"$select": sel, "$filter": "effectiveLatestChange eq true"},
            )
            used_select = sel
            break
        except Exception as e:
            last_err = e

    if used_select is None:
        raise RuntimeError(f"Unable to fetch EmpJob with any select candidate. Last error: {last_err}")

    contingent_source = used_select

    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    contingent_worker_count = 0
    contingent_workers_sample = []

    def is_contingent_job(j: dict) -> bool:
        # Try multiple fields; tenant can differ.
        raw = j.get("employeeClass") or j.get("employmentType") or j.get("employeeType") or ""
        s = str(raw).strip().lower()
        if not s:
            return False
        return (
            s in ("c", "contingent", "contingent worker", "contractor")
            or ("conting" in s)
            or ("contract" in s)
        )

    for j in jobs:
        uid = j.get("userId")
        mgr = j.get("managerId")

        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append({"userId": uid, "managerId": mgr})

        missing_fields = [k for k in ORG_FIELDS if is_blank(j.get(k))]
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

        if is_contingent_job(j):
            contingent_worker_count += 1
            if len(contingent_workers_sample) < MAX_SAMPLE:
                contingent_workers_sample.append(
                    {
                        "userId": uid,
                        "employeeClass": j.get("employeeClass"),
                        "employeeType": j.get("employeeType"),
                        "employmentType": j.get("employmentType"),
                    }
                )

    # Percent helper
    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    # Risk score (simple)
    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    # Output (keep alias keys Streamlit expects)
    metrics: Dict[str, Any] = {
        "snapshot_time_utc": now.isoformat(),

        "instance_url": instance_url,
        "api_base_url": api_base_url,
        "company_id": company_id or "",

        "active_users": total_active,
        "inactive_users": inactive_user_count,
        "inactive_user_count": inactive_user_count,

        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "contingent_workers": contingent_worker_count,
        "contingent_worker_count": contingent_worker_count,
        "contingent_source": contingent_source,

        "risk_score": risk_score,

        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }

    return metrics
