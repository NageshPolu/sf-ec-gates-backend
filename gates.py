from datetime import datetime, timezone
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------
# Small helpers
# ---------------------------
def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def _as_str(v: Any) -> str:
    return "" if v is None else str(v)


def _norm_id(v: Any) -> str:
    # normalize IDs so we can compare userId consistently
    return _as_str(v).strip()


def _truthy(v: Any) -> bool:
    s = _as_str(v).strip().lower()
    return s in ("t", "true", "1", "y", "yes")


def is_missing_email_value(v) -> bool:
    """
    Treat common placeholder strings as "missing email".
    """
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _first_nonempty_str(*vals: Any) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _extract_label_from_nav(nav: Any) -> Optional[str]:
    """
    SuccessFactors picklist nav shapes vary a lot. This tries multiple known patterns.
    """
    if not isinstance(nav, dict):
        return None

    # Common direct string fields
    direct = _first_nonempty_str(
        nav.get("label"),
        nav.get("name"),
        nav.get("value"),
        nav.get("description"),
        nav.get("defaultValue"),
    )
    if direct:
        return direct

    # Sometimes "label" is an object with defaultValue/value
    for key in ("label", "name", "value"):
        v = nav.get(key)
        if isinstance(v, dict):
            inner = _first_nonempty_str(
                v.get("defaultValue"),
                v.get("value"),
                v.get("label"),
                v.get("name"),
            )
            if inner:
                return inner

    # Sometimes there is a labels/results array
    for key in ("picklistLabels", "PicklistLabels", "labels", "Labels"):
        pl = nav.get(key)
        if isinstance(pl, dict) and isinstance(pl.get("results"), list):
            for item in pl["results"]:
                if isinstance(item, dict):
                    inner = _first_nonempty_str(
                        item.get("label"),
                        item.get("value"),
                        item.get("name"),
                        item.get("defaultValue"),
                    )
                    if inner:
                        return inner

    return None


def _extract_emplstatus(job: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (code, label) for EmpJob.emplStatus using best-effort expansion.
    """
    code = None
    label = None

    raw = job.get("emplStatus")
    if isinstance(raw, dict):
        code = _first_nonempty_str(raw.get("externalCode"), raw.get("code"), raw.get("value"))
    else:
        code = _as_str(raw).strip() if raw is not None else None

    nav = job.get("emplStatusNav")
    if isinstance(nav, dict):
        # code might also be here
        nav_code = _first_nonempty_str(nav.get("externalCode"), nav.get("code"), nav.get("value"))
        if nav_code:
            code = nav_code
        label = _extract_label_from_nav(nav)

    if code is not None and not str(code).strip():
        code = None

    return code, label


def _status_display(code: Optional[str], label: Optional[str]) -> str:
    if label and code:
        return f"{label} ({code})"
    if label:
        return label
    if code:
        return str(code)
    return "Unknown"


# ---------------------------
# Main function
# ---------------------------
def run_ec_gates(sf) -> dict:
    """
    EC Go-Live Gates snapshot with drilldowns (API-only).

    - Active users: based on User.status
    - Email hygiene: based on active users
    - Org/manager checks: based on EmpJob latest rows (scoped to active userIds where possible)
    - Inactive users: best-effort based on EmpJob.emplStatus (Name (Code))
    - Contingent workers: based on EmpEmployment.isContingentWorker (best), fallback to EmpJob hints
    """
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    errors: List[str] = []

    # ---------------------------
    # USERS (Active + Email Hygiene)
    # ---------------------------
    try:
        users = sf.get_all(
            "/odata/v2/User",
            {"$select": "userId,status,email,username"},
        )
    except Exception as e:
        users = []
        errors.append(f"User fetch failed: {type(e).__name__}: {e}")

    def is_active_user(u: dict) -> bool:
        s = _as_str(u.get("status")).strip().lower()
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user(u)]
    active_user_ids = {_norm_id(u.get("userId")) for u in active_users if _norm_id(u.get("userId"))}
    total_active = len(active_users)

    # Missing + duplicate emails (active users only)
    missing_email_sample = []
    email_to_users = defaultdict(list)

    for u in active_users:
        uid = _norm_id(u.get("userId"))
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

    duplicate_email_count = sum(
        (len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1
    )

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append(
                {"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]}
            )
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # EMPJOB (Latest records) with safe fallbacks
    # ---------------------------
    # Keep EmpJob selects conservative to avoid 400s.
    empjob_base = (
        "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange"
    )

    # Best-effort include emplStatus and expand the nav so we can show "Name (Code)"
    empjob_variants = [
        # Try expand + select nav (may fail on some tenants)
        {
            "$select": empjob_base + ",emplStatus,emplStatusNav/externalCode,emplStatusNav/label",
            "$expand": "emplStatusNav",
            "$filter": "effectiveLatestChange eq true",
        },
        # Try expand without nav select
        {
            "$select": empjob_base + ",emplStatus",
            "$expand": "emplStatusNav",
            "$filter": "effectiveLatestChange eq true",
        },
        # Try without expand
        {
            "$select": empjob_base + ",emplStatus",
            "$filter": "effectiveLatestChange eq true",
        },
        # Ultimate fallback (no emplStatus)
        {
            "$select": empjob_base,
            "$filter": "effectiveLatestChange eq true",
        },
    ]

    jobs: List[Dict[str, Any]] = []
    empjob_source = "EmpJob"
    employee_status_source = "fallback(User.status)"

    last_empjob_err = None
    for params in empjob_variants:
        try:
            jobs = sf.get_all("/odata/v2/EmpJob", params)
            # if we managed to include emplStatus, set source accordingly
            if "emplStatus" in params.get("$select", ""):
                employee_status_source = "EmpJob.emplStatus + emplStatusNav(best-effort)"
            else:
                employee_status_source = "fallback(User.status)"
            break
        except Exception as e:
            last_empjob_err = e
            jobs = []

    if last_empjob_err and not jobs:
        errors.append(f"EmpJob fetch failed: {type(last_empjob_err).__name__}: {last_empjob_err}")

    # Org/manager checks (scope to active userIds if available)
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    # Inactive users by employee status (best-effort)
    inactive_user_count = 0
    inactive_users_sample = []
    inactive_status_counter = Counter()

    # Identify "active" employee status if we have labels; otherwise fallback to userId membership
    def is_active_employee(job: Dict[str, Any]) -> bool:
        uid = _norm_id(job.get("userId"))
        # If we have label, trust it
        code, label = _extract_emplstatus(job)
        if label:
            # treat "Active" as active, avoid misclassifying "Inactive"
            l = label.strip().lower()
            if l == "active" or l.startswith("active "):
                return True
            if "inactive" in l:
                return False
        # fallback: if userId is active in User table
        if active_user_ids:
            return uid in active_user_ids
        # last resort: treat unknown as active (so we don't zero out your counts)
        return True

    for j in jobs:
        uid = _norm_id(j.get("userId"))
        if not uid:
            continue

        active_emp = is_active_employee(j)

        # Inactive population based on employee status logic
        if not active_emp:
            code, label = _extract_emplstatus(j)
            disp = _status_display(code, label)
            inactive_user_count += 1
            inactive_status_counter[disp] += 1
            if len(inactive_users_sample) < MAX_SAMPLE:
                inactive_users_sample.append(
                    {
                        "userId": uid,
                        "employeeStatus": disp,
                        "emplStatusCode": code,
                        "emplStatusName": label,
                    }
                )

        # Gate checks should apply to ACTIVE population (otherwise terminated/retired will pollute results)
        if not active_emp:
            continue

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
    # CONTINGENT WORKERS (Prefer EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_worker_count = 0
    contingent_workers_sample = []
    contingent_source = "EmpEmployment.isContingentWorker"

    empemployment_rows: List[Dict[str, Any]] = []
    empemployment_variants = [
        {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        {"$select": "userId,isContingentWorker"},
    ]

    last_empemployment_err = None
    for params in empemployment_variants:
        try:
            empemployment_rows = sf.get_all("/odata/v2/EmpEmployment", params)
            break
        except Exception as e:
            last_empemployment_err = e
            empemployment_rows = []

    if empemployment_rows:
        for r in empemployment_rows:
            uid = _norm_id(r.get("userId"))
            if not uid:
                continue
            if _truthy(r.get("isContingentWorker")):
                contingent_worker_count += 1
                if len(contingent_workers_sample) < MAX_SAMPLE:
                    contingent_workers_sample.append(
                        {
                            "userId": uid,
                            "isContingentWorker": r.get("isContingentWorker"),
                        }
                    )
        # If we used the "filter eq true" variant, all returned should be contingent; but we still guard.
    else:
        # Fallback (best-effort) using EmpJob hints ONLY if EmpEmployment is unavailable
        contingent_source = "fallback(EmpJob employeeClass/employeeType/employmentType)"
        if last_empemployment_err:
            errors.append(
                f"EmpEmployment fetch failed (fallback used): {type(last_empemployment_err).__name__}: {last_empemployment_err}"
            )

        def is_contingent_job(job: Dict[str, Any]) -> bool:
            raw = (
                job.get("employeeClass")
                or job.get("employmentType")
                or job.get("employeeType")
                or ""
            )
            s = _as_str(raw).strip().lower()
            if not s:
                return False
            return (
                s in ("c", "contingent", "contingent worker", "contractor")
                or ("conting" in s)
                or ("contract" in s)
            )

        for j in jobs:
            uid = _norm_id(j.get("userId"))
            if not uid:
                continue
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
    # Risk score (same style, but based on ACTIVE population)
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

    # Nice breakdown for inactive statuses (top few)
    inactive_status_breakdown = [
        {"employeeStatus": k, "count": v}
        for k, v in inactive_status_counter.most_common(20)
    ]

    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # Top KPIs
        "active_users": total_active,
        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        # Workforce KPIs
        "inactive_users": inactive_user_count,
        "inactive_user_count": inactive_user_count,

        "contingent_workers": contingent_worker_count,
        "contingent_worker_count": contingent_worker_count,

        # Sources (shown in UI)
        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,

        # Drilldowns
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "inactive_status_breakdown": inactive_status_breakdown,

        "contingent_workers_sample": contingent_workers_sample,

        # Debug/errors (optional to show in UI)
        "errors": errors,
        "sf_sources": {
            "empjob_source": empjob_source,
            "employee_status_source": employee_status_source,
            "contingent_source": contingent_source,
        },
    }

    return metrics
