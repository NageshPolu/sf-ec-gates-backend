from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Tuple


GATES_VERSION = "gates-2025-12-18-v9"


# ---------------------------
# Helpers
# ---------------------------
def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def safe_lower(v: Any) -> str:
    return "" if v is None else str(v).strip().lower()


def pick_any(d: dict, keys: List[str], default=None):
    for k in keys:
        if k in d and d.get(k) is not None and str(d.get(k)).strip() != "":
            return d.get(k)
    return default


def _pick_first_localized_label(nav_obj: dict) -> str:
    """
    Many tenants expose labels as:
      - label_defaultValue
      - labelDefaultValue
      - label_en_US (or other label_xx_YY)
      - label
      - name / value / description
    We pick the first non-empty we find.
    """
    if not isinstance(nav_obj, dict):
        return ""

    # 1) common known keys
    label = pick_any(
        nav_obj,
        [
            "label_defaultValue",
            "labelDefaultValue",
            "label",
            "name",
            "value",
            "description",
            "localizedLabel",
        ],
        "",
    )
    if label:
        return str(label).strip()

    # 2) any label_* key (label_en_US, label_de_DE, etc.)
    for k, v in nav_obj.items():
        if isinstance(k, str) and k.lower().startswith("label_") and v is not None and str(v).strip() != "":
            return str(v).strip()

    return ""


def _extract_status_from_nav(nav_obj: Any) -> Tuple[str, str]:
    """
    Returns: (code, name)
    """
    if not isinstance(nav_obj, dict):
        return "", ""
    code = pick_any(nav_obj, ["externalCode", "code", "id"], "") or ""
    name = _pick_first_localized_label(nav_obj) or ""
    return str(code).strip(), str(name).strip()


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
    Fetch EmpJob latest rows with progressive fallbacks (avoid 400 errors).
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

    try_fields_sets = [
        base_fields + ["emplStatus"],  # try status code
        base_fields,                   # fallback
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

    return [], meta


def fetch_empjob_status_labels(sf, errors: List[str]) -> Dict[str, str]:
    """
    Best-effort map: emplStatusCode -> label
    Uses EmpJob $expand=emplStatusNav and tries multiple label fields.
    """
    status_map: Dict[str, str] = {}

    # Try a richer select: some tenants use labelDefaultValue or label_en_US
    select_str = (
        "userId,emplStatus,"
        "emplStatusNav/externalCode,"
        "emplStatusNav/label_defaultValue,"
        "emplStatusNav/labelDefaultValue,"
        "emplStatusNav/label_en_US,"
        "emplStatusNav/label"
    )

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
        code_str = "" if code is None else str(code).strip()
        nav = r.get("emplStatusNav")

        nav_code, nav_name = _extract_status_from_nav(nav)
        # Prefer nav_name; otherwise keep empty (we'll fallback later)
        if code_str and nav_name:
            status_map[code_str] = nav_name

        # Sometimes the nav externalCode is the usable one, but code_str is numeric.
        # We'll still keep mapping by code_str because your KPI/sample uses it.
        if code_str and not nav_name and isinstance(nav, dict):
            # last resort: if nav has ANY label_* field, extract it
            guessed = _pick_first_localized_label(nav)
            if guessed:
                status_map[code_str] = guessed

    return status_map


def fetch_contingent_workers(sf, errors: List[str], max_sample: int) -> Tuple[int, List[dict], str]:
    """
    Prefer EmpEmployment.isContingentWorker (matches your report column).
    """
    rows = try_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        errors,
        label="EmpEmployment(isContingentWorker)",
    )
    if rows is not None:
        sample = [{"userId": r.get("userId"), "isContingentWorker": r.get("isContingentWorker")} for r in rows[:max_sample]]
        return len(rows), sample, "EmpEmployment.isContingentWorker"

    return 0, [], "not-available (no EmpEmployment access)"


# ---------------------------
# Main
# ---------------------------
def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)
    print(f"[GATES] version={GATES_VERSION} at {now.isoformat()}")

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
        s = safe_lower(u.get("status"))
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user_status(u)]
    inactive_users_user_status = [u for u in users if not is_active_user_status(u)]

    total_active_users = len(active_users)
    total_users = len(users)
    inactive_user_count_user_status = len(inactive_users_user_status)

    # Email hygiene (active only)
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
                {"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]}
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
    # Employee Status labels + Inactive users
    # ---------------------------
    status_label_map = fetch_empjob_status_labels(sf, errors)

    inactive_users_sample: List[dict] = []
    employee_status_source = empjob_meta.get("employee_status_source") or "fallback(User.status)"

    # Decide inactive count based on EmpJob status *only if* we can resolve at least some labels
    if jobs and status_label_map:
        inactive_count = 0
        for j in jobs:
            code = j.get("emplStatus")
            code_str = "" if code is None else str(code).strip()
            name = status_label_map.get(code_str, "")

            # Build the formatted "Name (Code)"
            if name:
                display = f"{name} ({code_str})" if code_str else name
            else:
                display = f"Unknown ({code_str})" if code_str else "Unknown"

            # Inactive = anything not starting with "active"
            if not display.lower().startswith("active"):
                inactive_count += 1
                if len(inactive_users_sample) < MAX_SAMPLE:
                    inactive_users_sample.append(
                        {
                            "userId": j.get("userId"),
                            "employeeStatus": display,   # ✅ Name (Code)
                            "emplStatusCode": code_str,  # ✅ code
                            "emplStatusName": name or "Unknown",  # ✅ name
                        }
                    )

        inactive_users_count_final = inactive_count
        employee_status_source = "EmpJob.emplStatus + emplStatusNav(best-effort)"

    else:
        # Fallback to User.status (still stable)
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
            employee_status_source = "EmpJob.emplStatus (labels blocked) -> fallback(User.status)"

    # ---------------------------
    # Contingent workers
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
    # Risk score
    # ---------------------------
    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active_users)) * 100))
    risk_score = min(100, risk)

    # ---------------------------
    # Output (keys Streamlit reads)
    # ---------------------------
    metrics = {
        "gates_version": GATES_VERSION,
        "snapshot_time_utc": now.isoformat(),

        "total_users": total_users,
        "active_users": total_active_users,
        "inactive_users_user_status": inactive_user_count_user_status,

        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        # Workforce
        "inactive_users": int(inactive_users_count_final or 0),
        "inactive_users_sample": inactive_users_sample,
        "employee_status_source": employee_status_source,

        "contingent_workers": contingent_worker_count,
        "contingent_workers_sample": contingent_workers_sample,
        "contingent_source": contingent_source,

        "risk_score": risk_score,

        # Samples
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,

        # Debug
        "empjob_select_used": empjob_meta.get("empjob_select_used"),
        "errors": errors[:50],
    }

    return metrics
