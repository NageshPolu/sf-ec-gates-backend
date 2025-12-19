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
def _try_get_all(sf, path: str, params: Dict[str, Any], note: str) -> Tuple[bool, str, List[dict], Optional[str]]:
    """
    Returns (ok, note, rows, err)
    """
    try:
        rows = sf.get_all(path, params)
        return True, note, rows, None
    except Exception as e:
        return False, note, [], str(e)


def _fetch_empjob_latest(sf) -> Tuple[List[dict], str, bool, List[str]]:
    """
    Returns: (jobs, employee_status_source, has_emplstatus, diagnostics)
    """
    diagnostics: List[str] = []
    base = "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange"

    # Try 1: emplStatus + nav (labels)
    ok, note, rows, err = _try_get_all(
        sf,
        "/odata/v2/EmpJob",
        {
            "$select": base
            + ",emplStatus,emplStatusNav/externalName,emplStatusNav/name,emplStatusNav/externalCode",
            "$expand": "emplStatusNav",
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpJob.emplStatus + emplStatusNav",
    )
    if ok and rows:
        return rows, note, True, diagnostics
    if err:
        diagnostics.append(f"EmpJob nav expand failed: {err[:300]}")

    # Try 2: codes only
    ok, note, rows, err = _try_get_all(
        sf,
        "/odata/v2/EmpJob",
        {"$select": base + ",emplStatus", "$filter": "effectiveLatestChange eq true"},
        "EmpJob.emplStatus (codes only)",
    )
    if ok and rows:
        return rows, note, True, diagnostics
    if err:
        diagnostics.append(f"EmpJob codes-only failed: {err[:300]}")

    # Try 3: no emplStatus
    ok, note, rows, err = _try_get_all(
        sf,
        "/odata/v2/EmpJob",
        {"$select": base, "$filter": "effectiveLatestChange eq true"},
        "EmpJob (no emplStatus)",
    )
    if ok and rows:
        return rows, note, False, diagnostics
    if err:
        diagnostics.append(f"EmpJob base failed: {err[:300]}")

    return [], "EmpJob (failed)", False, diagnostics


def _fetch_empemployment_contingent(sf) -> Tuple[Dict[str, bool], str, List[str]]:
    """
    Returns (userId->isContingentWorker, source_note, diagnostics)
    """
    diagnostics: List[str] = []

    # Try 1: effectiveLatestChange exists in most tenants
    ok, note, rows, err = _try_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {
            "$select": "userId,isContingentWorker,effectiveLatestChange",
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpEmployment.isContingentWorker (latest)",
    )
    if ok and rows:
        m: Dict[str, bool] = {}
        for r in rows:
            uid = r.get("userId")
            if uid is not None:
                m[str(uid)] = as_bool(r.get("isContingentWorker"))
        return m, note, diagnostics
    if err:
        diagnostics.append(f"EmpEmployment latest failed: {err[:300]}")

    # Try 2: no effectiveLatestChange (some tenants)
    ok, note, rows, err = _try_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {"$select": "userId,isContingentWorker"},
        "EmpEmployment.isContingentWorker (no latest filter)",
    )
    if ok and rows:
        m = {}
        for r in rows:
            uid = r.get("userId")
            if uid is not None:
                m[str(uid)] = as_bool(r.get("isContingentWorker"))
        return m, note, diagnostics
    if err:
        diagnostics.append(f"EmpEmployment no-filter failed: {err[:300]}")

    return {}, "not-available (no EmpEmployment/isContingentWorker)", diagnostics


def _fetch_user_rows(sf) -> Tuple[List[dict], List[str]]:
    diagnostics: List[str] = []
    ok, note, rows, err = _try_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
        "User",
    )
    if ok and rows is not None:
        return rows, diagnostics
    if err:
        diagnostics.append(f"User fetch failed: {err[:300]}")
    return [], diagnostics


def _fetch_emplstatus_catalog(sf) -> Tuple[Dict[str, str], str]:
    """
    Best-effort lookup table for emplStatus code -> name.
    Different tenants expose different entities; we try a few.
    If nothing works, returns empty mapping.
    """
    candidates = [
        # Common-ish guesses; harmless if they 400/403.
        ("/odata/v2/FOEmploymentStatus", {"$select": "externalCode,name,externalName"}, "FOEmploymentStatus"),
        ("/odata/v2/FOEmployeeStatus", {"$select": "externalCode,name,externalName"}, "FOEmployeeStatus"),
        # Some tenants store picklist options (names differ); best effort.
        ("/odata/v2/FOPicklistOption", {"$select": "externalCode,name,externalName,picklistId"}, "FOPicklistOption"),
    ]

    for path, params, label in candidates:
        ok, _, rows, _ = _try_get_all(sf, path, params, label)
        if not ok or not rows:
            continue

        m: Dict[str, str] = {}
        for r in rows:
            code = r.get("externalCode")
            name = r.get("externalName") or r.get("name")
            if code is not None and name:
                m[str(code).strip()] = str(name).strip()

        if m:
            return m, label

    return {}, "not-available"


def _emplstatus_name_code(j: dict, code_to_name: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Try to get (name, code) from EmpJob row.
    - code usually is j['emplStatus']
    - name maybe from j['emplStatusNav'] or from catalog lookup
    """
    code = j.get("emplStatus")
    name = None
    nav = j.get("emplStatusNav")
    if isinstance(nav, dict):
        name = nav.get("externalName") or nav.get("name")

    code_s = str(code).strip() if code is not None else None
    if not name and code_s and code_to_name:
        name = code_to_name.get(code_s)

    name_s = str(name).strip() if name else None
    return name_s, code_s


# -----------------------------
# MAIN
# -----------------------------
def run_ec_gates(sf, instance_url: str | None = None, api_base_url: str | None = None) -> dict:
    """
    EC gates snapshot (API-only).
    Data sources:
      - EmpJob (latest) for workforce + org + manager + employee status
      - EmpEmployment (latest) for contingent (best)
      - User for email hygiene + fallback active/inactive if employee status name not available
    """
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # 1) EmpJob
    jobs, employee_status_source, has_emplstatus, empjob_diag = _fetch_empjob_latest(sf)
    if not jobs:
        # Hard fail: prevents saving "all zeros" snapshots.
        raise RuntimeError(
            "EmpJob returned 0 rows. Check API base URL (api*.domain), credentials, and OData permissions for EmpJob."
        )

    # 2) Users
    users, user_diag = _fetch_user_rows(sf)
    if not users:
        raise RuntimeError(
            "User returned 0 rows. Check OData permissions for User and whether the API base URL is correct."
        )

    users_by_id = {str(u.get("userId")): u for u in users if u.get("userId") is not None}

    # 3) Status catalog (only needed if labels blocked)
    code_to_name, status_catalog_source = ({}, "not-available")
    if has_emplstatus:
        # If nav labels are blocked, code_to_name may help.
        code_to_name, status_catalog_source = _fetch_emplstatus_catalog(sf)

    # 4) Contingent map (best)
    contingent_map, contingent_source, empemp_diag = _fetch_empemployment_contingent(sf)

    # ---------------------------
    # Workforce status: Active/Inactive
    # ---------------------------
    active_job_rows: List[dict] = []
    inactive_job_rows: List[dict] = []

    status_name_coverage = 0
    status_counter = Counter()

    for j in jobs:
        uid = str(j.get("userId")) if j.get("userId") is not None else None

        name, code = _emplstatus_name_code(j, code_to_name)
        if name:
            status_name_coverage += 1

        display = f"{name} ({code})" if name and code else (code or name or "Unknown")
        status_counter[display] += 1

        # Decide active/inactive:
        # Prefer name if available; else fall back to User.status (reliable boolean-ish)
        if name:
            is_active = (name.strip().lower() == "active")
        else:
            u = users_by_id.get(uid or "", {})
            s = str(u.get("status", "")).strip().lower()
            is_active = s in ("active", "t", "true", "1")

        (active_job_rows if is_active else inactive_job_rows).append(j)

    active_user_ids = [str(j.get("userId")) for j in active_job_rows if j.get("userId") is not None]
    inactive_user_ids = [str(j.get("userId")) for j in inactive_job_rows if j.get("userId") is not None]

    total_active = len(active_user_ids)
    inactive_user_count = len(inactive_user_ids)

    inactive_users_sample = []
    for j in inactive_job_rows[:MAX_SAMPLE]:
        name, code = _emplstatus_name_code(j, code_to_name)
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
                missing_email_sample.append({"userId": uid, "email": email, "username": u.get("username")})
        else:
            email_to_users[email.lower()].append(uid)

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
    # OUTPUT
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        "instance_url": instance_url,
        "api_base_url": api_base_url,

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

        # sources / diagnostics
        "employee_status_source": employee_status_source,
        "status_catalog_source": status_catalog_source,
        "contingent_source": contingent_source,
        "emplstatus_label_coverage": {"rows_with_label": status_name_coverage, "total_rows": len(jobs)},
        "inactive_by_status": dict(status_counter),
        "diagnostics": {
            "empjob": empjob_diag,
            "user": user_diag,
            "empemployment": empemp_diag,
        },

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
