# gates.py
from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict, Counter
from typing import Any, Dict, List, Tuple, Optional


# -----------------------------
# small helpers
# -----------------------------
def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    """
    Treat common placeholder strings as "missing email".
    """
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def as_bool(v) -> bool:
    if v is True:
        return True
    if v is False:
        return False
    s = str(v).strip().lower()
    return s in ("t", "true", "1", "y", "yes")


# -----------------------------
# OData fetch "best-effort"
# -----------------------------
def _try_get_all(sf, path: str, params: Dict[str, Any], note: str) -> Tuple[bool, str, List[dict]]:
    try:
        rows = sf.get_all(path, params)
        return True, note, rows
    except Exception:
        return False, note, []


def _fetch_empjob_latest(sf) -> Tuple[List[dict], str, bool]:
    """
    Returns: (jobs, employee_status_source, has_emplstatus)
    """
    base = (
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange"
    )
    # Try: status label via nav
    ok, note, rows = _try_get_all(
        sf,
        "/odata/v2/EmpJob",
        {
            "$select": base + ",emplStatus,emplStatusNav/externalName,emplStatusNav/name,emplStatusNav/externalCode",
            "$expand": "emplStatusNav",
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpJob.emplStatus + emplStatusNav",
    )
    if ok and rows:
        return rows, note, True

    # Try: status codes only
    ok, note, rows = _try_get_all(
        sf,
        "/odata/v2/EmpJob",
        {
            "$select": base + ",emplStatus",
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpJob.emplStatus (codes only)",
    )
    if ok and rows:
        return rows, note, True

    # Fallback: no emplStatus
    ok, note, rows = _try_get_all(
        sf,
        "/odata/v2/EmpJob",
        {
            "$select": base,
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpJob (no emplStatus)",
    )
    return rows, note, False


def _fetch_empemployment_contingent(sf) -> Tuple[Dict[str, bool], str]:
    """
    Returns map: userId -> isContingentWorker
    """
    ok, note, rows = _try_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {
            "$select": "userId,isContingentWorker,effectiveLatestChange",
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpEmployment.isContingentWorker",
    )
    if ok and rows:
        m = {}
        for r in rows:
            uid = r.get("userId")
            if uid:
                m[str(uid)] = as_bool(r.get("isContingentWorker"))
        return m, note

    return {}, "not-available (no EmpEmployment/isContingentWorker)"


def _fetch_user_rows(sf) -> List[dict]:
    ok, _, rows = _try_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
        "User",
    )
    return rows or []


def _fallback_contingent_from_empjob(j: dict) -> bool:
    raw = (j.get("employeeClass") or j.get("employmentType") or j.get("employeeType") or "")
    s = str(raw).strip().lower()
    if not s:
        return False
    return (
        s in ("c", "contingent", "contingent worker", "contractor")
        or ("conting" in s)
        or ("contract" in s)
    )


def _emplstatus_name_code(j: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Try to get (name, code) from EmpJob row.
    - code usually is j['emplStatus']
    - name maybe from j['emplStatusNav']
    """
    code = j.get("emplStatus")
    name = None
    nav = j.get("emplStatusNav")
    if isinstance(nav, dict):
        name = nav.get("externalName") or nav.get("name")
        # sometimes the nav has externalCode; but we keep j['emplStatus'] as the code-of-record
    return (str(name).strip() if name else None, str(code).strip() if code is not None else None)


# -----------------------------
# MAIN
# -----------------------------
def run_ec_gates(sf, instance_url: str | None = None, api_base_url: str | None = None) -> dict:
    """
    EC gates snapshot (API-only).
    Data sources:
      - EmpJob (latest) for workforce + org + manager + employee status
      - EmpEmployment (latest) for contingent (best)
      - User for email hygiene + fallback active/inactive if employee status not available
    """
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # 1) EmpJob
    jobs, employee_status_source, has_emplstatus = _fetch_empjob_latest(sf)

    # 2) Contingent map
    contingent_map, contingent_source = _fetch_empemployment_contingent(sf)

    # 3) Users (for email + fallback)
    users = _fetch_user_rows(sf)
    users_by_id = {str(u.get("userId")): u for u in users if u.get("userId") is not None}

    # ---------------------------
    # Workforce status: Active/Inactive
    # ---------------------------
    # If we can resolve status name: Active means name == "Active"
    # Else: fallback to User.status
    active_job_rows: List[dict] = []
    inactive_job_rows: List[dict] = []

    status_name_coverage = 0
    status_counter = Counter()

    if has_emplstatus:
        for j in jobs:
            name, code = _emplstatus_name_code(j)
            if name:
                status_name_coverage += 1
            disp = f"{name} ({code})" if name and code else (code or name or "Unknown")
            status_counter[disp] += 1

            # Decide active/inactive
            if name:
                is_active = (name.strip().lower() == "active")
            else:
                # if we only have code, we cannot reliably decide "Active" vs "Terminated"
                # -> fallback to User.status for that employee
                uid = str(j.get("userId"))
                u = users_by_id.get(uid, {})
                s = str(u.get("status", "")).strip().lower()
                is_active = s in ("active", "t", "true", "1")

            (active_job_rows if is_active else inactive_job_rows).append(j)
    else:
        # No emplStatus at all -> fallback to User.status
        active_ids = set()
        inactive_ids = set()
        for u in users:
            uid = str(u.get("userId"))
            s = str(u.get("status", "")).strip().lower()
            if s in ("active", "t", "true", "1"):
                active_ids.add(uid)
            else:
                inactive_ids.add(uid)

        for j in jobs:
            uid = str(j.get("userId"))
            (active_job_rows if uid in active_ids else inactive_job_rows).append(j)

        employee_status_source = "fallback(User.status)"

    active_user_ids = [str(j.get("userId")) for j in active_job_rows if j.get("userId") is not None]
    inactive_user_ids = [str(j.get("userId")) for j in inactive_job_rows if j.get("userId") is not None]

    total_active = len(active_user_ids)
    inactive_user_count = len(inactive_user_ids)

    # sample inactive with status label/code
    inactive_users_sample = []
    for j in inactive_job_rows[:MAX_SAMPLE]:
        name, code = _emplstatus_name_code(j)
        inactive_users_sample.append(
            {
                "userId": j.get("userId"),
                "emplStatusCode": code,
                "emplStatusName": name,
                "emplStatusDisplay": f"{name} ({code})" if name and code else (code or name),
            }
        )

    # ---------------------------
    # Email hygiene (ACTIVE employees only)
    # ---------------------------
    missing_email_sample = []
    email_to_users = defaultdict(list)

    missing_email_count = 0
    for uid in active_user_ids:
        u = users_by_id.get(uid, {})
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        if is_missing_email_value(email):
            missing_email_count += 1
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append(
                    {"userId": uid, "email": email, "username": u.get("username")}
                )
        else:
            email_to_users[email.lower()].append(uid)

    duplicate_email_count = sum((len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1)

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append({"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]})
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # Org & manager gates (ACTIVE employees only)
    # ---------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    for j in active_job_rows:
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
    # Contingent gate
    # ---------------------------
    contingent_worker_count = 0
    contingent_workers_sample = []

    if contingent_map:
        for uid in active_user_ids + inactive_user_ids:
            if contingent_map.get(uid) is True:
                contingent_worker_count += 1
                if len(contingent_workers_sample) < MAX_SAMPLE:
                    contingent_workers_sample.append({"userId": uid, "isContingentWorker": True})
    else:
        # fallback (not perfect)
        for j in jobs:
            if _fallback_contingent_from_empjob(j):
                contingent_worker_count += 1
                if len(contingent_workers_sample) < MAX_SAMPLE:
                    contingent_workers_sample.append(
                        {
                            "userId": j.get("userId"),
                            "employeeClass": j.get("employeeClass"),
                            "employeeType": j.get("employeeType"),
                            "employmentType": j.get("employmentType"),
                        }
                    )

    # ---------------------------
    # Percent + Risk score (based on ACTIVE employees)
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

    # ---------------------------
    # OUTPUT (keys Streamlit expects)
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # helps multi-instance filtering
        "instance_url": instance_url,
        "api_base_url": api_base_url,

        # KPIs
        "active_users": total_active,
        "inactive_users": inactive_user_count,
        "empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "contingent_workers": contingent_worker_count,
        "risk_score": risk_score,

        # sources / diagnostics (to show in UI as a caption)
        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,
        "emplstatus_label_coverage": {
            "rows_with_label": status_name_coverage,
            "total_rows": len(jobs),
        },
        "inactive_by_status": dict(status_counter),

        # Drilldowns
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }

    # alias keys (backward compatibility)
    metrics["inactive_user_count"] = metrics["inactive_users"]
    metrics["current_empjob_rows"] = metrics["empjob_rows"]
    metrics["contingent_worker_count"] = metrics["contingent_workers"]

    return metrics
