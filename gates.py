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


def extract_scalar(v):
    """
    SF sometimes returns navigation/code fields as dicts.
    Try to extract a meaningful scalar when possible.
    """
    if isinstance(v, dict):
        for k in ("code", "externalCode", "value", "id"):
            if k in v:
                return v[k]
    return v


def norm(v) -> str:
    if v is None:
        return ""
    return str(extract_scalar(v)).strip().lower()


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


def is_active_from_user_status(v) -> Optional[bool]:
    """
    User.status is NOT consistent across tenants (sometimes always 'active' for visible users).
    We use it only as a fallback.
    """
    s = norm(v)
    if s in ("active", "a", "true", "t", "1", "yes", "y"):
        return True
    if s in ("inactive", "i", "false", "f", "0", "no", "n"):
        return False
    return None  # unknown


def is_active_from_emplstatus(v) -> Optional[bool]:
    """
    EmpJob.emplStatus is usually a code like 'A' for Active.
    Treat anything non-empty and not 'A' as inactive (terminated/retired/etc).
    """
    s = norm(v)
    if not s:
        return None
    if s in ("a", "active"):
        return True
    return False


def truthy_sf_bool(v) -> Optional[bool]:
    """
    Normalize SF boolean-like fields that may come back as true/false, "true"/"false", 1/0, "t"/"f".
    """
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = norm(v)
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return None


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
    # USERS (Email hygiene base list)
    # ---------------------------
    users = safe_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
    )

    # ---------------------------
    # EMPJOB (Latest records) with safe fallback
    # ---------------------------
    # Some tenants throw 400 if you request a field that doesn't exist.
    # We'll try progressively smaller selects until it works.
    empjob_select_candidates = [
        # Rich (includes emplStatus + contingent flags where available)
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange,employeeClass,employeeType,employmentType,isContingentWorker",
        # Without isContingentWorker
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange,employeeClass,employeeType,employmentType",
        # Without employeeType/employmentType
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange,employeeClass",
        # Without emplStatus
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange,employeeClass,employeeType,employmentType,isContingentWorker",
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

    employee_status_source = "EmpJob.emplStatus -> fallback(User.status)"
    contingent_source = "EmpJob.isContingentWorker/employeeClass/employeeType/employmentType (best-effort)"

    # ---------------------------
    # OPTIONAL: EmpEmployment.isContingentWorker (more reliable in many tenants)
    # ---------------------------
    # Not all tenants expose EmpEmployment or the field; handle gracefully.
    contingent_by_empemployment: dict[str, bool] = {}
    empemployment_available = False
    empemployment_err = None

    try:
        empemployment = safe_get_all(
            sf,
            "/odata/v2/EmpEmployment",
            {"$select": "userId,isContingentWorker"},
        )
        empemployment_available = True
        for r in empemployment:
            uid = r.get("userId")
            if not uid:
                continue
            b = truthy_sf_bool(r.get("isContingentWorker"))
            if b is not None:
                contingent_by_empemployment[uid] = b
        if empemployment_available:
            contingent_source = "EmpEmployment.isContingentWorker -> fallback(EmpJob fields)"
    except Exception as e:
        empemployment_err = str(e)

    # ---------------------------
    # STATUS: ACTIVE vs INACTIVE (prefer EmpJob.emplStatus)
    # ---------------------------
    job_status_by_user: dict[str, Any] = {}
    emplstatus_value_counts = defaultdict(int)

    for j in jobs:
        uid = j.get("userId")
        es = j.get("emplStatus")
        if uid:
            job_status_by_user[uid] = es
        s = norm(es)
        if s:
            emplstatus_value_counts[s] += 1

    active_users: List[dict] = []
    inactive_users: List[dict] = []
    inactive_users_sample: List[dict] = []
    unknown_status_user_count = 0

    for u in users:
        uid = u.get("userId")

        # 1) Prefer EmpJob.emplStatus
        job_es = job_status_by_user.get(uid)
        a = is_active_from_emplstatus(job_es)

        # 2) Fallback to User.status
        if a is None:
            a = is_active_from_user_status(u.get("status"))

        # 3) If still unknown, track it (don’t silently inflate inactive)
        if a is None:
            unknown_status_user_count += 1
            a = True  # safest default

        if a:
            active_users.append(u)
        else:
            inactive_users.append(u)
            if len(inactive_users_sample) < MAX_SAMPLE:
                inactive_users_sample.append(
                    {
                        "userId": uid,
                        "emplStatus": job_es,
                        "status": u.get("status"),
                        "email": u.get("email"),
                        "username": u.get("username"),
                    }
                )

    total_active = len(active_users)
    inactive_user_count = len(inactive_users)

    # ---------------------------
    # Email hygiene (among ACTIVE users)
    # ---------------------------
    missing_email_sample: List[dict] = []
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
    # ORG + MANAGER checks + CONTINGENT (from EmpJob/EmpEmployment)
    # ---------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    missing_manager_sample: List[dict] = []
    invalid_org_sample: List[dict] = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    contingent_worker_count = 0
    contingent_workers_sample: List[dict] = []

    def is_contingent_job(j: dict) -> bool:
        uid = j.get("userId")

        # 1) Prefer EmpEmployment.isContingentWorker if we have it
        if uid and uid in contingent_by_empemployment:
            return bool(contingent_by_empemployment[uid])

        # 2) Try EmpJob.isContingentWorker if exists
        b = truthy_sf_bool(j.get("isContingentWorker"))
        if b is not None:
            return b

        # 3) Fallback heuristics
        raw = j.get("employeeClass") or j.get("employmentType") or j.get("employeeType") or ""
        s = norm(raw)
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
                        "isContingentWorker": j.get("isContingentWorker"),
                        "employeeClass": j.get("employeeClass"),
                        "employeeType": j.get("employeeType"),
                        "employmentType": j.get("employmentType"),
                    }
                )

    # ---------------------------
    # Percent helper
    # ---------------------------
    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    # ---------------------------
    # Risk score (simple)
    # ---------------------------
    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    # ---------------------------
    # Output (keep alias keys Streamlit expects)
    # ---------------------------
    metrics: Dict[str, Any] = {
        "snapshot_time_utc": now.isoformat(),

        "instance_url": instance_url,
        "api_base_url": api_base_url,
        "company_id": company_id or "",

        # workforce
        "active_users": total_active,
        "inactive_users": inactive_user_count,
        "inactive_user_count": inactive_user_count,

        # EmpJob
        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        # checks
        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "contingent_workers": contingent_worker_count,
        "contingent_worker_count": contingent_worker_count,

        "risk_score": risk_score,

        # samples
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        # debug / explainability
        "employee_status_source": employee_status_source,
        "emplstatus_value_counts": dict(emplstatus_value_counts),
        "unknown_status_user_count": unknown_status_user_count,
        "contingent_source": contingent_source,
        "empjob_select_used": used_select,
        "empemployment_available": empemployment_available,
        "empemployment_error": empemployment_err or "",
    }

    return metrics
