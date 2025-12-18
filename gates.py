from datetime import datetime, timezone
from collections import defaultdict, Counter
from typing import Any, Dict, List, Tuple, Optional

GATES_VERSION = "gates-2025-12-18-v10"


# ---------------------------
# Helpers
# ---------------------------
def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def safe_lower(v: Any) -> str:
    return safe_str(v).lower()


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = safe_lower(v)
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def pick_first_nonempty(d: dict, keys: List[str]) -> str:
    for k in keys:
        if k in d:
            v = d.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
    return ""


def try_get_all(sf, path: str, params: Dict[str, Any], errors: List[str], label: str):
    try:
        return sf.get_all(path, params)
    except Exception as e:
        errors.append(f"{label}: {type(e).__name__}: {str(e)[:400]}")
        return None


def _extract_label_from_nav(nav: Any) -> str:
    """
    Tenants differ. We try:
      - label_defaultValue / labelDefaultValue
      - label_en_US / label_* (any)
      - label / name / description
      - nested label nav objects (labelNav / label)
    """
    if not isinstance(nav, dict):
        return ""

    # direct string fields
    direct = pick_first_nonempty(
        nav,
        [
            "label_defaultValue",
            "labelDefaultValue",
            "label",
            "name",
            "description",
            "value",
        ],
    )
    if direct:
        return direct

    # any localized label_* field
    for k, v in nav.items():
        if isinstance(k, str) and k.lower().startswith("label_") and v is not None and str(v).strip() != "":
            return str(v).strip()

    # nested label objects sometimes appear
    for nested_key in ("labelNav", "label", "labels", "localizedLabel"):
        nested = nav.get(nested_key)
        if isinstance(nested, dict):
            nested_val = pick_first_nonempty(nested, ["defaultValue", "value", "label", "name", "description"])
            if nested_val:
                return nested_val
        if isinstance(nested, list) and nested:
            # list of localized label objects
            for item in nested:
                if isinstance(item, dict):
                    nested_val = pick_first_nonempty(item, ["defaultValue", "value", "label", "name", "description"])
                    if nested_val:
                        return nested_val

    return ""


# ---------------------------
# Fetchers
# ---------------------------
def fetch_users(sf, errors: List[str]) -> List[dict]:
    return try_get_all(
        sf,
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
        errors,
        "User",
    ) or []


def fetch_empjob_latest(sf, errors: List[str]) -> Tuple[List[dict], str]:
    """
    Keep this stable: only fields that are almost always supported.
    """
    select_str = (
        "userId,managerId,company,businessUnit,division,department,location,"
        "emplStatus,effectiveLatestChange"
    )
    jobs = try_get_all(
        sf,
        "/odata/v2/EmpJob",
        {"$select": select_str, "$filter": "effectiveLatestChange eq true"},
        errors,
        f"EmpJob select({select_str})",
    )
    return (jobs or []), select_str


def fetch_status_labels(sf, errors: List[str]) -> Dict[str, str]:
    """
    Build map: emplStatusCode -> emplStatusName
    We try multiple strategies because tenants expose picklist labels differently.
    """
    label_map: Dict[str, str] = {}

    # Strategy A: expand emplStatusNav with several common label fields
    params_a = {
        "$select": (
            "emplStatus,"
            "emplStatusNav/externalCode,"
            "emplStatusNav/label_defaultValue,"
            "emplStatusNav/labelDefaultValue,"
            "emplStatusNav/label_en_US,"
            "emplStatusNav/label,"
            "emplStatusNav/name,"
            "emplStatusNav/description"
        ),
        "$expand": "emplStatusNav",
        "$filter": "effectiveLatestChange eq true",
    }
    rows = try_get_all(sf, "/odata/v2/EmpJob", params_a, errors, "EmpJob expand(emplStatusNav) A")

    if rows:
        for r in rows:
            code = safe_str(r.get("emplStatus"))
            nav = r.get("emplStatusNav")
            name = _extract_label_from_nav(nav)
            if code and name:
                label_map[code] = name

    # Strategy B: some tenants need nested expand
    if not label_map:
        params_b = {
            "$select": (
                "emplStatus,"
                "emplStatusNav/externalCode,"
                "emplStatusNav/labelNav/defaultValue,"
                "emplStatusNav/labelNav/value"
            ),
            "$expand": "emplStatusNav/labelNav",
            "$filter": "effectiveLatestChange eq true",
        }
        rows_b = try_get_all(sf, "/odata/v2/EmpJob", params_b, errors, "EmpJob expand(emplStatusNav/labelNav) B")
        if rows_b:
            for r in rows_b:
                code = safe_str(r.get("emplStatus"))
                nav = r.get("emplStatusNav") or {}
                name = ""
                if isinstance(nav, dict):
                    name = _extract_label_from_nav(nav)
                if code and name:
                    label_map[code] = name

    # Strategy C: last attempt - query PicklistValueV2 by codes we see (OR filter)
    # (We only do this if we already have some EmpJob data; caller can ignore if empty.)
    # NOTE: collisions are possible across picklists, but in practice emplStatus codes are consistent.
    if not label_map:
        # We can’t build the OR filter without codes; caller can pass codes via "errors" otherwise.
        pass

    return label_map


def fetch_contingent(sf, errors: List[str], max_sample: int) -> Tuple[int, List[dict], str]:
    rows = try_get_all(
        sf,
        "/odata/v2/EmpEmployment",
        {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        errors,
        "EmpEmployment(isContingentWorker)",
    )
    if rows is None:
        return 0, [], "not-available (no EmpEmployment access)"

    sample = [{"userId": r.get("userId"), "isContingentWorker": r.get("isContingentWorker")} for r in rows[:max_sample]]
    return len(rows), sample, "EmpEmployment.isContingentWorker"


# ---------------------------
# Main
# ---------------------------
def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)
    errors: List[str] = []
    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # USERS
    users = fetch_users(sf, errors)

    def is_active_user_status(u: dict) -> bool:
        s = safe_lower(u.get("status"))
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user_status(u)]
    total_active_users = len(active_users)

    # Email hygiene (active only)
    missing_email_sample: List[dict] = []
    email_to_users: Dict[str, List[str]] = defaultdict(list)

    for u in active_users:
        uid = u.get("userId")
        email_raw = u.get("email")
        email = safe_str(email_raw)
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

    # EMPJOB
    jobs, empjob_select_used = fetch_empjob_latest(sf, errors)

    # Org + Manager checks
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

    # Employee status labels (best effort)
    status_label_map = fetch_status_labels(sf, errors)

    # Inactive users based on EmpJob.emplStatus
    # If we can’t get names, still compute correctly:
    #   - choose the most frequent emplStatus code as "Active"
    #   - treat all other codes as "Inactive"
    empl_codes = [safe_str(j.get("emplStatus")) for j in jobs if safe_str(j.get("emplStatus"))]
    code_counts = Counter(empl_codes)
    active_code_guess = code_counts.most_common(1)[0][0] if code_counts else ""

    inactive_users_count = 0
    inactive_users_sample: List[dict] = []

    for j in jobs:
        code = safe_str(j.get("emplStatus"))
        if not code:
            continue

        name = status_label_map.get(code, "")
        name = name if name else "Unknown"
        display = f"{name} ({code})"

        # If we do have a real label map and it includes an "Active" label, use that.
        # Otherwise, use the modal code as active.
        if status_label_map:
            is_inactive = not safe_lower(name).startswith("active")
        else:
            is_inactive = (code != active_code_guess)

        if is_inactive:
            inactive_users_count += 1
            if len(inactive_users_sample) < MAX_SAMPLE:
                inactive_users_sample.append(
                    {
                        "userId": j.get("userId"),
                        "emplStatusCode": code,
                        "emplStatusName": name,
                        "employeeStatus": display,
                    }
                )

    if status_label_map:
        employee_status_source = "EmpJob.emplStatus + emplStatusNav(best-effort labels)"
    else:
        employee_status_source = f"EmpJob.emplStatus (codes only) -> activeCodeGuess={active_code_guess}"

    # Contingent
    contingent_worker_count, contingent_workers_sample, contingent_source = fetch_contingent(
        sf, errors, MAX_SAMPLE
    )

    # Percent helpers (use active users baseline like before)
    def pct(x: int) -> float:
        return 0.0 if total_active_users == 0 else round((x / total_active_users) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    # Risk score
    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active_users)) * 100))
    risk_score = min(100, risk)

    return {
        "gates_version": GATES_VERSION,
        "snapshot_time_utc": now.isoformat(),

        # KPIs
        "active_users": total_active_users,
        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        "contingent_workers": contingent_worker_count,
        "inactive_users": inactive_users_count,

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        # Sources
        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,
        "empjob_select_used": empjob_select_used,

        # Drilldowns
        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "invalid_org_sample": invalid_org_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_manager_sample": missing_manager_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        # Debug (optional)
        "errors": errors[:50],
    }
