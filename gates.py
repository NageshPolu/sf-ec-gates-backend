from datetime import datetime, timezone
from collections import defaultdict


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    """Treat common placeholder strings as missing email."""
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _norm_status(v) -> str:
    return "" if v is None else str(v).strip().lower()


def _is_active_employee_status(v: str) -> bool:
    """
    Employee Status from EmpJob.emplStatus / emplStatusNav.label.
    Excel shows values like: Active, Terminated, Retired, Discarded, Paid Leave, Unpaid Leave.
    Treat only Active as active.
    """
    s = _norm_status(v)
    if not s:
        return False
    return s in ("active", "a", "t", "true", "1")


def run_ec_gates(sf) -> dict:
    """
    EC Go-Live Gates snapshot with drilldowns (API-only).
    Sources:
      - Users: /odata/v2/User (emails, usernames)
      - Jobs:  /odata/v2/EmpJob (latest) incl emplStatus for employee status
      - Contingent: /odata/v2/EmpEmployment (isContingentWorker) if available; fallback heuristics
    """
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # USERS (for email + usernames)
    # ---------------------------
    users = sf.get_all(
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
    )
    user_by_id = {u.get("userId"): u for u in users if u.get("userId")}

    # Fallback active definition from User.status (only used if EmpJob employee status is unavailable)
    def _is_user_active(u: dict) -> bool:
        s = _norm_status(u.get("status"))
        return s in ("active", "t", "true", "1")

    # ---------------------------
    # EMPJOB (latest) + Employee Status
    # ---------------------------
    # Try to fetch employee status label via navigation; if not supported, fall back to emplStatus field only.
    base_select = (
        "userId,managerId,company,businessUnit,division,department,location,"
        "effectiveLatestChange,emplStatus"
    )

    jobs = []
    employee_status_source = "EmpJob.emplStatus"
    try:
        # Many tenants support emplStatusNav for label/externalCode
        jobs = sf.get_all(
            "/odata/v2/EmpJob",
            {
                "$select": base_select,
                "$expand": "emplStatusNav",
                "$filter": "effectiveLatestChange eq true",
            },
        )
        employee_status_source = "EmpJob.emplStatusNav.label (fallback EmpJob.emplStatus)"
    except Exception:
        jobs = sf.get_all(
            "/odata/v2/EmpJob",
            {"$select": base_select, "$filter": "effectiveLatestChange eq true"},
        )

    # Determine employee status value per job row
    def job_employee_status(j: dict) -> str:
        nav = j.get("emplStatusNav") or {}
        # Common fields in nav objects: label / externalCode / picklist labels
        return (
            nav.get("label")
            or nav.get("externalCode")
            or j.get("emplStatus")
            or ""
        )

    # Build active/inactive sets based on Employee Status (from jobs)
    # If the status is missing for almost all rows (permissions/field), fallback to User.status.
    status_values = [job_employee_status(j) for j in jobs]
    non_blank_status_count = sum(1 for s in status_values if _norm_status(s))

    active_user_ids = set()
    inactive_user_ids = set()

    if non_blank_status_count >= max(5, int(0.1 * max(1, len(jobs)))):
        # Use Employee Status from EmpJob
        for j in jobs:
            uid = j.get("userId")
            if not uid:
                continue
            s = job_employee_status(j)
            if _is_active_employee_status(s):
                active_user_ids.add(uid)
            else:
                inactive_user_ids.add(uid)

        # If someone appears in both (multiple employments), keep them active
        inactive_user_ids -= active_user_ids
    else:
        # Fallback: use User.status
        employee_status_source = "Fallback: User.status (EmpJob employee status not available)"
        for uid, u in user_by_id.items():
            if _is_user_active(u):
                active_user_ids.add(uid)
            else:
                inactive_user_ids.add(uid)

    total_active = len(active_user_ids)
    inactive_user_count = len(inactive_user_ids)

    # Inactive sample (show employee status if we have it)
    inactive_users_sample = []
    if inactive_user_ids:
        # Create a quick map of latest status seen in jobs
        latest_status_by_uid = {}
        for j in jobs:
            uid = j.get("userId")
            if uid and uid not in latest_status_by_uid:
                latest_status_by_uid[uid] = job_employee_status(j)

        for uid in list(inactive_user_ids)[:MAX_SAMPLE]:
            u = user_by_id.get(uid, {})
            inactive_users_sample.append(
                {
                    "userId": uid,
                    "employeeStatus": latest_status_by_uid.get(uid, ""),
                    "userStatus": u.get("status"),
                    "email": u.get("email"),
                    "username": u.get("username"),
                }
            )

    # ---------------------------
    # Email hygiene (ONLY among active employees)
    # ---------------------------
    missing_email_sample = []
    email_to_users = defaultdict(list)

    for uid in active_user_ids:
        u = user_by_id.get(uid) or {}
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

    missing_email_count = sum(
        1 for uid in active_user_ids
        if is_missing_email_value((user_by_id.get(uid) or {}).get("email"))
    )

    duplicate_email_count = sum(
        (len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1
    )

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
    # Org + Manager checks (ONLY among active employees)
    # ---------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0
    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    # We may have multiple EmpJob rows per userId; evaluate rows that belong to active users
    for j in jobs:
        uid = j.get("userId")
        if not uid or uid not in active_user_ids:
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

    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    # ---------------------------
    # Contingent workers (prefer EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_worker_count = 0
    contingent_workers_sample = []
    contingent_source = "EmpEmployment.isContingentWorker"

    try:
        cont = sf.get_all(
            "/odata/v2/EmpEmployment",
            {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        )
        contingent_ids = []
        for r in cont:
            uid = r.get("userId")
            if uid:
                contingent_ids.append(uid)

        contingent_unique = list(dict.fromkeys(contingent_ids))  # keep order, unique
        contingent_worker_count = len(contingent_unique)
        contingent_workers_sample = [{"userId": uid, "isContingentWorker": True} for uid in contingent_unique[:MAX_SAMPLE]]

    except Exception:
        # Fallback heuristic if EmpEmployment not accessible
        contingent_source = "Fallback: EmpJob.employeeClass/employeeType/employmentType (heuristic)"
        try:
            jobs2 = sf.get_all(
                "/odata/v2/EmpJob",
                {
                    "$select": "userId,effectiveLatestChange,employeeClass,employeeType,employmentType",
                    "$filter": "effectiveLatestChange eq true",
                },
            )
            def is_contingent_job(j: dict) -> bool:
                raw = (j.get("employeeClass") or j.get("employmentType") or j.get("employeeType") or "")
                s = _norm_status(raw)
                return (
                    s in ("c", "contingent", "contingent worker", "contractor")
                    or ("conting" in s)
                    or ("contract" in s)
                )

            cont_ids = []
            for j in jobs2:
                uid = j.get("userId")
                if uid and is_contingent_job(j):
                    cont_ids.append(uid)

            cont_unique = list(dict.fromkeys(cont_ids))
            contingent_worker_count = len(cont_unique)
            contingent_workers_sample = [{"userId": uid} for uid in cont_unique[:MAX_SAMPLE]]
        except Exception:
            contingent_source = "not-available"
            contingent_worker_count = 0
            contingent_workers_sample = []

    # ---------------------------
    # Risk score (simple + explainable)
    # ---------------------------
    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    # ---------------------------
    # Output (keys Streamlit expects)
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        "active_users": total_active,
        "inactive_users": inactive_user_count,

        "empjob_rows": len(jobs),
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        "contingent_workers": contingent_worker_count,

        # Sources (for transparency)
        "employee_status_source": employee_status_source,
        "contingent_source": contingent_source,

        # Drilldowns
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }

    return metrics
