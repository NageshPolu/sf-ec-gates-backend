from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional


# -----------------------------
# Small helpers
# -----------------------------
def is_blank(v: Any) -> bool:
    return v is None or str(v).strip() == ""


def _to_str(v: Any) -> str:
    return "" if v is None else str(v)


def _norm(s: Any) -> str:
    return _to_str(s).strip().lower()


def _as_bool(v: Any) -> bool:
    s = _norm(v)
    return s in ("true", "t", "1", "y", "yes", "active")


def is_missing_email_value(v: Any) -> bool:
    if v is None:
        return True
    s = _norm(v)
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _pick_first_label_from_picklistlabels(obj: Dict[str, Any]) -> Optional[str]:
    """
    Some tenants return labels under:
      emplStatusNav: { picklistLabels: { results: [ { label: 'Terminated', locale: 'en_US' }, ... ] } }
    This tries to pick en_US first, otherwise first available.
    """
    pl = obj.get("picklistLabels")
    if not isinstance(pl, dict):
        return None
    results = pl.get("results")
    if not isinstance(results, list) or not results:
        return None

    # prefer en_US
    for r in results:
        if _norm(r.get("locale")) == "en_us":
            lab = r.get("label") or r.get("value") or r.get("name")
            if lab:
                return str(lab)

    # fallback: first row
    r0 = results[0]
    lab = r0.get("label") or r0.get("value") or r0.get("name")
    return str(lab) if lab else None


def _emplstatus_name_from_nav(nav: Any) -> Optional[str]:
    """
    Tries many common shapes:
      - { label: 'Terminated' }
      - { name: 'Terminated' }
      - { externalName: 'Terminated' }
      - { picklistLabels: { results: [...] } }
    """
    if not isinstance(nav, dict):
        return None

    # common keys
    for k in ("label", "name", "externalName", "value", "displayName"):
        if nav.get(k):
            return str(nav.get(k))

    # picklistLabels structure
    pl = _pick_first_label_from_picklistlabels(nav)
    if pl:
        return pl

    return None


def _fmt_status(name: Optional[str], code: Optional[str]) -> str:
    n = (name or "").strip()
    c = (code or "").strip()
    if n and c:
        return f"{n} ({c})"
    if c:
        return c
    if n:
        return n
    return ""


# -----------------------------
# Main gates
# -----------------------------
def run_ec_gates(sf) -> dict:
    """
    EC Go-Live / Health Gates snapshot with drilldowns (API-only).

    Key improvements:
    - Inactive users based on EmpJob.emplStatus name != "Active" (best effort).
    - emplStatus shown as "Name (Code)".
    - Contingent workers based on EmpEmployment.isContingentWorker (best match to report).
    - Email samples returned with both key naming conventions to avoid UI mismatches.
    - Avoids selecting fields that cause EmpJob 400 in some tenants.
    """
    now = datetime.now(timezone.utc)

    # Keep payload safe
    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    errors: List[str] = []

    # ---------------------------
    # USERS (Active + email hygiene)
    # ---------------------------
    users: List[Dict[str, Any]] = []
    try:
        users = sf.get_all("/odata/v2/User", {"$select": "userId,status,email,username"})
    except Exception as e:
        errors.append(f"User query failed: {e}")

    def is_active_user(u: dict) -> bool:
        # In many tenants, status is boolean or string-ish.
        # Accept a few representations.
        return _as_bool(u.get("status"))

    active_users = [u for u in users if is_active_user(u)]
    total_active = len(active_users)

    # Email checks
    missing_email_sample: List[Dict[str, Any]] = []
    email_to_users: Dict[str, List[str]] = defaultdict(list)

    for u in active_users:
        uid = _to_str(u.get("userId")).strip()
        email_raw = u.get("email")
        email = "" if email_raw is None else str(email_raw).strip()
        email_norm = email.lower()

        if is_missing_email_value(email):
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append(
                    {"userId": uid, "email": email, "username": u.get("username")}
                )
            continue

        email_to_users[email_norm].append(uid)

    missing_email_count = sum(
        1 for u in active_users if is_missing_email_value(u.get("email"))
    )

    duplicate_email_count = sum(
        (len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1
    )

    dup_rows: List[Dict[str, Any]] = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append(
                {"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]}
            )
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # EMPJOB (Latest records + emplStatus)
    # ---------------------------
    jobs: List[Dict[str, Any]] = []
    employee_status_source = "EmpJob.emplStatus (codes only)"

    # Try best-effort: include emplStatusNav label/name via expand
    # Not all tenants allow selecting nav properties; we try a few variants.
    empjob_base_fields = [
        "userId",
        "managerId",
        "company",
        "businessUnit",
        "division",
        "department",
        "location",
        "effectiveLatestChange",
        "emplStatus",
    ]

    # Variant A: try direct label/name on nav
    tried_empjob_variants: List[Tuple[Dict[str, str], str]] = []

    # A1
    tried_empjob_variants.append((
        {
            "$select": ",".join(empjob_base_fields + ["emplStatusNav/externalCode", "emplStatusNav/label", "emplStatusNav/name"]),
            "$expand": "emplStatusNav",
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpJob.emplStatus + emplStatusNav(label/name)",
    ))

    # A2 (nested picklistLabels)
    tried_empjob_variants.append((
        {
            "$select": ",".join(empjob_base_fields + ["emplStatusNav/externalCode"]),
            "$expand": "emplStatusNav/picklistLabels",
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpJob.emplStatus + emplStatusNav/picklistLabels(best-effort)",
    ))

    # A3 codes only
    tried_empjob_variants.append((
        {
            "$select": ",".join(empjob_base_fields),
            "$filter": "effectiveLatestChange eq true",
        },
        "EmpJob.emplStatus (codes only) -> fallback(User.status) if needed",
    ))

    last_empjob_error: Optional[str] = None
    for params, source_label in tried_empjob_variants:
        try:
            jobs = sf.get_all("/odata/v2/EmpJob", params)
            employee_status_source = source_label
            last_empjob_error = None
            break
        except Exception as e:
            last_empjob_error = str(e)
            jobs = []

    if last_empjob_error:
        errors.append(f"EmpJob query failed (all variants): {last_empjob_error}")

    # ---------------------------
    # Contingent workers (EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_source = "EmpEmployment.isContingentWorker"
    empemployment: List[Dict[str, Any]] = []
    contingent_by_user: Dict[str, bool] = {}

    try:
        # Keep it minimal; many tenants support these
        empemployment = sf.get_all("/odata/v2/EmpEmployment", {"$select": "userId,isContingentWorker"})
        for r in empemployment:
            uid = _to_str(r.get("userId")).strip()
            contingent_by_user[uid] = _as_bool(r.get("isContingentWorker"))
    except Exception as e:
        contingent_source = "not-available (EmpEmployment not readable)"
        errors.append(f"EmpEmployment query failed: {e}")

    # ---------------------------
    # Compute checks from EmpJob
    # ---------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    inactive_users_count = 0
    contingent_worker_count = 0

    missing_manager_sample: List[Dict[str, Any]] = []
    invalid_org_sample: List[Dict[str, Any]] = []
    inactive_users_sample: List[Dict[str, Any]] = []
    contingent_workers_sample: List[Dict[str, Any]] = []

    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    # Build status code -> name map (best-effort from nav)
    status_name_by_code: Dict[str, Optional[str]] = {}

    def extract_status_code(job: Dict[str, Any]) -> str:
        # Often emplStatus itself is already a code/externalCode (string or numeric)
        v = job.get("emplStatus")
        return _to_str(v).strip()

    def extract_status_name(job: Dict[str, Any]) -> Optional[str]:
        nav = job.get("emplStatusNav")
        return _emplstatus_name_from_nav(nav)

    # Populate map from observed rows
    for j in jobs:
        code = extract_status_code(j)
        if not code:
            continue
        if code not in status_name_by_code:
            status_name_by_code[code] = extract_status_name(j)

    # Determine if a job is inactive by status label (preferred)
    def is_inactive_by_emplstatus(code: str) -> Optional[bool]:
        """
        Returns:
          True/False if we can decide,
          None if we can't (no names anywhere).
        """
        if not code:
            return None
        name = status_name_by_code.get(code)
        if name is None:
            return None

        s = name.strip().lower()
        # Treat exactly "active" as active; everything else considered inactive
        # (matches your report-style classification where Terminated/Retired/Leave/Discarded are not active)
        return s != "active"

    # If we have ZERO status names resolved, we’ll fallback to User.status for inactive count (but still show codes)
    have_any_status_names = any(v for v in status_name_by_code.values())

    inactive_users_fallback_count = 0
    if users:
        inactive_users_fallback_count = sum(1 for u in users if not is_active_user(u))

    # Iterate jobs and compute checks
    for j in jobs:
        uid = _to_str(j.get("userId")).strip()
        mgr = j.get("managerId")

        # Manager check
        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append({"userId": uid, "managerId": mgr})

        # Org check
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

        # Contingent check (EmpEmployment map)
        if contingent_by_user.get(uid) is True:
            contingent_worker_count += 1
            if len(contingent_workers_sample) < MAX_SAMPLE:
                contingent_workers_sample.append({"userId": uid, "isContingentWorker": True})

        # Inactive users by emplStatus (preferred)
        code = extract_status_code(j)
        name = status_name_by_code.get(code) if code else None
        inactive_flag = is_inactive_by_emplstatus(code) if have_any_status_names else None

        if inactive_flag is True:
            inactive_users_count += 1
            if len(inactive_users_sample) < MAX_SAMPLE:
                inactive_users_sample.append(
                    {
                        "userId": uid,
                        "emplStatusCode": code,
                        "emplStatusName": name,
                        "employeeStatus": _fmt_status(name, code),
                    }
                )

    # If we couldn't resolve any status names at all, fallback inactive_users to User.status
    inactive_users_method = "EmpJob.emplStatus (name != Active)"
    if not have_any_status_names:
        inactive_users_method = "fallback(User.status) (emplStatus names unavailable)"
        inactive_users_count = inactive_users_fallback_count
        # Provide a small sample from User
        if not inactive_users_sample and users:
            for u in users:
                if not is_active_user(u) and len(inactive_users_sample) < MAX_SAMPLE:
                    inactive_users_sample.append(
                        {
                            "userId": u.get("userId"),
                            "userStatus": u.get("status"),
                            "employeeStatus": "Inactive (User.status)",
                        }
                    )

    # ---------------------------
    # Percent helpers + risk score
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
    # OUTPUT (include aliases for UI compatibility)
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # KPIs
        "active_users": total_active,
        "empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "inactive_users": inactive_users_count,
        "contingent_workers": contingent_worker_count,

        "risk_score": risk_score,

        # Sources / methods
        "employee_status_source": employee_status_source,
        "inactive_users_method": inactive_users_method,
        "contingent_source": contingent_source,

        # Drilldowns (primary keys)
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        # Aliases (some UIs look for pluralized keys)
        "missing_emails_sample": missing_email_sample,
        "duplicate_emails_sample": duplicate_email_sample,
        "missing_managers_sample": missing_manager_sample,

        # Diagnostics
        "errors": errors,
    }

    return metrics
