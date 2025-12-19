from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def safe_get_all(sf, path: str, params: dict) -> List[dict]:
    try:
        return sf.get_all(path, params)
    except Exception as e:
        raise RuntimeError(f"SF API error calling {path} with params={params}: {e}")


def _norm_picklist(v: Any) -> str:
    """
    EmpJob picklists sometimes come back as:
      - "A" / "I"
      - "Active" / "Inactive"
      - dict-like objects (rare, depending on select/expand)
    Normalize into a simple lowercase string.
    """
    if v is None:
        return ""
    if isinstance(v, dict):
        # common keys if it ever comes back expanded-ish
        for k in ("externalCode", "code", "value", "id", "picklistValue", "name"):
            if k in v and v[k] is not None:
                return str(v[k]).strip().lower()
        return str(v).strip().lower()
    return str(v).strip().lower()


def _emplstatus_is_active(empl_status_raw: Any) -> Optional[bool]:
    """
    Return True/False/None (None means unknown -> fallback to User.status).
    """
    s = _norm_picklist(empl_status_raw)
    if not s:
        return None

    # very common patterns
    if s in ("a", "active", "t", "true", "1"):
        return True
    if s in ("i", "inactive", "f", "false", "0"):
        return False

    # frequent real-world codes/words
    if "term" in s or "separat" in s or "inact" in s or "retir" in s:
        return False
    if "act" in s:
        return True

    return None


def _userstatus_is_active(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("active", "t", "true", "1")


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in ("true", "t", "1", "yes", "y")


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
    # EMPJOB (Latest) - primary scope + status + org + manager
    # ---------------------------
    # Keep fallback fields for contingent heuristic too
    empjob_select_candidates = [
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange,employeeClass,employeeType,employmentType",
        "userId,managerId,company,businessUnit,division,department,location,emplStatus,effectiveLatestChange",
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange",
    ]

    base_filter = "effectiveLatestChange eq true"
    if company_id and str(company_id).strip():
        base_filter = f"{base_filter} and company eq '{company_id.strip()}'"

    jobs: List[dict] = []
    used_select = None
    last_err = None

    for sel in empjob_select_candidates:
        try:
            jobs = safe_get_all(sf, "/odata/v2/EmpJob", {"$select": sel, "$filter": base_filter})
            used_select = sel
            break
        except Exception as e:
            last_err = e

    if used_select is None:
        raise RuntimeError(f"Unable to fetch EmpJob. Last error: {last_err}")

    scope_user_ids = {str(j.get("userId")).strip() for j in jobs if j.get("userId")}
    if not scope_user_ids:
        raise RuntimeError("EmpJob returned 0 rows for the given scope. Check company_id, permissions, or API base URL.")

    job_by_user = {str(j.get("userId")).strip(): j for j in jobs if j.get("userId")}

    # Debug: emplStatus value distribution
    emplstatus_counts = Counter()
    for j in jobs:
        emplstatus_counts[_norm_picklist(j.get("emplStatus"))] += 1
    emplstatus_value_counts = dict(emplstatus_counts)

    employee_status_source = "EmpJob.emplStatus (fallback(User.status) when unknown)"

    # ---------------------------
    # USERS (email + username, and fallback status)
    # ---------------------------
    users_all = safe_get_all(sf, "/odata/v2/User", {"$select": "userId,status,email,username"})
    users = []
    for u in users_all:
        uid = str(u.get("userId") or "").strip()
        if uid and uid in scope_user_ids:
            users.append(u)

    active_users: List[dict] = []
    inactive_users: List[dict] = []
    unknown_status_user_count = 0

    for u in users:
        uid = str(u.get("userId") or "").strip()
        j = job_by_user.get(uid, {})
        emp_is_active = _emplstatus_is_active(j.get("emplStatus"))

        if emp_is_active is None:
            unknown_status_user_count += 1
            is_active = _userstatus_is_active(u.get("status"))
        else:
            is_active = bool(emp_is_active)

        (active_users if is_active else inactive_users).append(u)

    total_active = len(active_users)
    inactive_user_count = len(inactive_users)

    inactive_users_sample = [
        {
            "userId": u.get("userId"),
            "emplStatus": job_by_user.get(str(u.get("userId") or "").strip(), {}).get("emplStatus"),
            "userStatus": u.get("status"),
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
        email = "" if u.get("email") is None else str(u.get("email")).strip()
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
    # Org + Manager checks from EmpJob
    # ---------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

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

    # ---------------------------
    # Contingent workers
    #   Primary: EmpEmployment.isContingentWorker (best)
    #   Fallback: EmpJob heuristics
    # ---------------------------
    contingent_workers_sample = []
    contingent_worker_count = 0
    contingent_source = "EmpEmployment.isContingentWorker"
    contingent_true_in_empemployment_total = 0

    contingent_user_ids: set[str] = set()
    try:
        empemps = safe_get_all(
            sf,
            "/odata/v2/EmpEmployment",
            {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        )
        for e in empemps:
            contingent_true_in_empemployment_total += 1
            uid = str(e.get("userId") or "").strip()
            if uid:
                contingent_user_ids.add(uid)

        scoped_contingents = [uid for uid in contingent_user_ids if uid in scope_user_ids]
        contingent_worker_count = len(scoped_contingents)

        for uid in scoped_contingents[:MAX_SAMPLE]:
            contingent_workers_sample.append({"userId": uid, "isContingentWorker": True})

    except Exception as e:
        contingent_source = f"EmpEmployment.isContingentWorker not available → fallback(EmpJob fields). Reason: {type(e).__name__}"
        def is_contingent_job(j: dict) -> bool:
            raw = j.get("employeeClass") or j.get("employmentType") or j.get("employeeType") or ""
            s = str(raw).strip().lower()
            if not s:
                return False
            return ("conting" in s) or ("contract" in s) or (s in ("c", "contractor", "contingent", "contingent worker"))

        for j in jobs:
            uid = str(j.get("userId") or "").strip()
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

    # ---------------------------
    # Percent helper + risk
    # ---------------------------
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

    return {
        "snapshot_time_utc": now.isoformat(),

        "instance_url": instance_url,
        "api_base_url": api_base_url,
        "company_id": company_id or "",

        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,

        # DEBUG helpers (check in Raw JSON)
        "emplstatus_value_counts": emplstatus_value_counts,
        "unknown_status_user_count": unknown_status_user_count,
        "contingent_true_in_empemployment_total": contingent_true_in_empemployment_total,

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

        "risk_score": risk_score,

        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }
