from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Tuple


# ✅ hard stamp so you can verify Render deployed the correct file
GATES_VERSION = "gates-2025-12-18-v8"


# ---------------------------
# Helpers
# ---------------------------
def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    """
    Treat common placeholder strings as "missing email" (tenant data hygiene).
    """
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def safe_lower(v: Any) -> str:
    return "" if v is None else str(v).strip().lower()


def pick_any(d: dict, keys: List[str], default=None):
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return default


def _extract_nav_label(nav_obj: Any) -> Tuple[str, str]:
    """
    Best-effort extract:
      - external code
      - label / name
    from emplStatusNav-like objects (varies by tenant).
    """
    if not isinstance(nav_obj, dict):
        return "", ""

    code = pick_any(nav_obj, ["externalCode", "code", "id"], "")
    # label_defaultValue is common for PicklistValueV2
    name = pick_any(
        nav_obj,
        ["label_defaultValue", "label", "name", "value", "localizedLabel", "description"],
        "",
    )
    return str(code or ""), str(name or "")


# ---------------------------
# OData fetch wrappers (robust)
# ---------------------------
def try_get_all(sf, path: str, params: Dict[str, Any], errors: List[str], label: str):
    try:
        return sf.get_all(path, params)
    except Exception as e:
        errors.append(f"{label}: {type(e).__name__}: {str(e)[:300]}")
        return None


def fetch_empjob_latest(sf, errors: List[str]) -> Tuple[List[dict], Dict[str, Any]]:
    """
    Fetch EmpJob latest rows with progressive fallbacks to avoid 400 errors when fields differ.
    Returns (jobs, meta)
    """
    meta = {"empjob_select_used": None, "employee_status_source": None}

    base_fields = [
        "userId",
        "managerId",
        "company",
        "businessUnit",
        "division",
        "department",
        "location",
        "effectiveLatestChange",
    ]

    # emplStatus causes 400 in some tenants; we try it first but safely fallback.
    try_fields_sets = [
        base_fields + ["emplStatus"],  # attempt employee status code
        base_fields,                   # fallback without emplStatus
    ]

    for fields in try_fields_sets:
        select_str = ",".join(fields)
        jobs = try_get_all(
            sf,
            "/odata/v2/EmpJob",
            {"$select": select_str, "$filter": "effectiveLatestChange eq true"},
            errors,
            label=f"EmpJob select({select_str})",
        )
        if jobs is not None:
            meta["empjob_select_used"] = select_str
            meta["employee_status_source"] = "EmpJob.emplStatus" if "emplStatus" in fields else "fallback(User.status)"
            return jobs, meta

    # ultimate fallback (should never happen)
    return [], meta


def fetch_empjob_status_labels(sf, errors: List[str]) -> Dict[str, str]:
    """
    Best-effort: try to fetch emplStatus label/name via $expand=emplStatusNav.
    Many tenants support it, some don't (permissions/entity differences).
    Returns map: emplStatusCode(str) -> "Name (Code)" or "Name (Code)".
    """
    status_map: Dict[str, str] = {}

    # Minimal safe list. If your tenant supports nav, this gives label.
    select_str = "userId,emplStatus,emplStatusNav/externalCode,emplStatusNav/label_defaultValue"
    params = {
        "$select": select_str,
        "$expand": "emplStatusNav",
        "$filter": "effectiveLatestChange eq true",
    }
    rows = try_get_all(sf, "/odata/v2/EmpJob", params, errors, label="EmpJob expand(emplStatusNav)")
    if not rows:
        return status_map

    for r in rows:
        code = r.get("emplStatus")
        nav = r.get("emplStatusNav")
        nav_code, nav_name = _extract_nav_label(nav)
        code_str = "" if code is None else str(code).strip()
        # Prefer nav label if present
        if nav_name:
            # If nav_code empty, show the actual emplStatus code
            show_code = nav_code.strip() if nav_code else code_str
            status_map[code_str] = f"{nav_name} ({show_code})" if show_code else nav_name

    return status_map


def fetch_contingent_workers(sf, errors: List[str], max_sample: int) -> Tuple[int, List[dict], str]:
    """
    Prefer EmpEmployment.isContingentWorker (matches your report column).
    """
    # Try EmpEmployment first
    params = {
        "$select": "userId,isContingentWorker",
        "$filter": "isContingentWorker eq true",
    }
    rows = try_get_all(sf, "/odata/v2/EmpEmployment", params, errors, label="EmpEmployment(isContingentWorker)")
    if rows is not None:
        sample = [{"userId": r.get("userId"), "isContingentWorker": r.get("isContingentWorker")} for r in rows[:max_sample]]
        return len(rows), sample, "EmpEmployment.isContingentWorker"

    # Fallback: cannot reliably compute from EmpJob if this entity not accessible
    return 0, [], "not-available (no EmpEmployment access)"


# ---------------------------
# Main
# ---------------------------
def run_ec_gates(sf) -> dict:
    """
    EC Go-Live Gates snapshot with drilldowns (API-only).
    Data sources:
      - User (email hygiene + active users baseline)
      - EmpJob (manager/org checks; optional emplStatus)
      - EmpEmployment (contingent workers)
    """
    now = datetime.now(timezone.utc)
    print(f"[GATES] version={GATES_VERSION} at {now.isoformat()}")

    # Keep payload safe
    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    errors: List[str] = []

    # ---------------------------
    # USERS (Active baseline + email hygiene)
    # ---------------------------
    users = try_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
        errors,
        label="User",
    ) or []

    def is_active_user_status(u: dict) -> bool:
        # Keep your old behavior (this is what previously gave 1399)
        s = safe_lower(u.get("status"))
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user_status(u)]
    inactive_users_user_status = [u for u in users if not is_active_user_status(u)]

    total_active_users = len(active_users)
    total_users = len(users)
    inactive_user_count_user_status = len(inactive_users_user_status)

    # Email hygiene (on ACTIVE user.status only — matches prior logic)
    missing_email_sample: List[dict] = []
    email_to_users: Dict[str, List[str]] = defaultdict(list)

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
                {
                    "email": email,
                    "count": len(uids),
                    "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL],
                }
            )
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # EMPJOB (Latest) - org + manager checks
    # ---------------------------
    jobs, empjob_meta = fetch_empjob_latest(sf, errors)

    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    missing_manager_sample: List[dict] = []
    invalid_org_sample: List[dict] = []
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
    # INACTIVE USERS (Employee Status if we can label it; else fallback to User.status)
    # ---------------------------
    # Try to build label map via emplStatusNav
    status_label_map = fetch_empjob_status_labels(sf, errors)

    inactive_users_sample: List[dict] = []
    inactive_users_count_final = None
    employee_status_source = empjob_meta.get("employee_status_source") or "fallback(User.status)"

    # If we have emplStatus codes AND at least some labels, compute inactive based on label != "Active"
    if jobs and status_label_map:
        # count jobs by status display
        inactive_by_emp_status = 0
        for j in jobs:
            code = j.get("emplStatus")
            code_str = "" if code is None else str(code).strip()
            display = status_label_map.get(code_str, f"Unknown ({code_str})" if code_str else "Unknown")

            # Decide “inactive”: anything that is not literally "Active (...)" (best effort)
            # (Works well once labels exist, like your Excel)
            if not display.lower().startswith("active"):
                inactive_by_emp_status += 1
                if len(inactive_users_sample) < MAX_SAMPLE:
                    inactive_users_sample.append(
                        {
                            "userId": j.get("userId"),
                            "emplStatus": display,   # Name (Code)
                            "emplStatusCode": code_str,
                        }
                    )

        inactive_users_count_final = inactive_by_emp_status
        employee_status_source = "EmpJob.emplStatus + emplStatusNav(best-effort)"

    else:
        # Fallback: reliable inactive from User.status
        inactive_users_count_final = inactive_user_count_user_status
        for u in inactive_users_user_status[:MAX_SAMPLE]:
            inactive_users_sample.append(
                {
                    "userId": u.get("userId"),
                    "status": u.get("status"),
                    "email": u.get("email"),
                    "username": u.get("username"),
                }
            )
        if jobs and not status_label_map:
            employee_status_source = "EmpJob.emplStatus (codes only) -> fallback(User.status)"

    # ---------------------------
    # CONTINGENT WORKERS (EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_worker_count, contingent_workers_sample, contingent_source = fetch_contingent_workers(
        sf, errors, MAX_SAMPLE
    )

    # ---------------------------
    # Percent helpers (use active_users baseline)
    # ---------------------------
    def pct(x: int) -> float:
        return 0.0 if total_active_users == 0 else round((x / total_active_users) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    # ---------------------------
    # Risk score (simple, explainable)
    # ---------------------------
    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active_users)) * 100))
    risk_score = min(100, risk)

    # ---------------------------
    # OUTPUT (keys your Streamlit reads)
    # ---------------------------
    metrics = {
        "gates_version": GATES_VERSION,
        "snapshot_time_utc": now.isoformat(),

        # Baselines
        "total_users": total_users,
        "active_users": total_active_users,  # keep stable (User.status)
        "inactive_users_user_status": inactive_user_count_user_status,

        # EmpJob volume
        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        # Manager + Org checks
        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "org_missing_field_counts": org_missing_field_counts,

        # Email hygiene
        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        # Workforce (inactive + contingent)
        "inactive_users": int(inactive_users_count_final or 0),
        "inactive_users_sample": inactive_users_sample,
        "employee_status_source": employee_status_source,

        "contingent_workers": contingent_worker_count,
        "contingent_workers_sample": contingent_workers_sample,
        "contingent_source": contingent_source,

        # Risk score
        "risk_score": risk_score,

        # Samples for other tabs
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,

        # Debug (safe)
        "empjob_select_used": empjob_meta.get("empjob_select_used"),
        "errors": errors[:50],  # cap
    }

    return metrics
