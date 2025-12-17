from datetime import datetime, timezone
from collections import defaultdict


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


def _norm_status(v) -> str:
    return "" if v is None else str(v).strip().lower()


def run_ec_gates(sf) -> dict:
    """
    EC Go-Live Gates snapshot with drilldowns (API-only).
    Uses:
      - EmpJob (latest) for employee status + org + manager checks
      - User for email hygiene
      - EmpEmployment for contingent worker flag (isContingentWorker)
    """
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ------------------------------------------------------------
    # 1) EmpJob (latest) - include employee status (emplStatus)
    # ------------------------------------------------------------
    base_select = (
        "userId,managerId,company,businessUnit,division,department,location,"
        "effectiveLatestChange,emplStatus"
    )

    # Try to also fetch label via navigation if tenant supports it
    # (If this fails, we fall back to raw emplStatus value)
    try:
        jobs = sf.get_all(
            "/odata/v2/EmpJob",
            {
                "$select": base_select + ",emplStatusNav/label",
                "$expand": "emplStatusNav",
                "$filter": "effectiveLatestChange eq true",
            },
        )
        empl_status_source = "EmpJob.emplStatusNav.label (fallback emplStatus)"
    except Exception:
        jobs = sf.get_all(
            "/odata/v2/EmpJob",
            {"$select": base_select, "$filter": "effectiveLatestChange eq true"},
        )
        empl_status_source = "EmpJob.emplStatus"

    # Helper: best-effort employee status label
    def job_status_label(j: dict) -> str:
        nav = j.get("emplStatusNav")
        if isinstance(nav, dict):
            lbl = nav.get("label")
            if lbl and str(lbl).strip():
                return str(lbl).strip()
        raw = j.get("emplStatus")
        return "" if raw is None else str(raw).strip()

    # Define what counts as "active" employee status (match your Excel view)
    ACTIVE_STATUS_TOKENS = {
        "active",
        "paid leave",
        "unpaid leave",
    }
    # also allow common code-style values if tenant returns codes
    ACTIVE_CODE_TOKENS = {"a", "active", "t", "true", "1"}  # keep tolerant

    def is_active_employee_status(label_or_code: str) -> bool:
        s = _norm_status(label_or_code)
        if not s:
            return False
        return (s in ACTIVE_STATUS_TOKENS) or (s in ACTIVE_CODE_TOKENS)

    # Build sets for active/inactive based on EmpJob
    active_jobs = []
    inactive_jobs = []

    inactive_by_status = defaultdict(int)

    for j in jobs:
        status_lbl = job_status_label(j)
        if is_active_employee_status(status_lbl):
            active_jobs.append(j)
        else:
            inactive_jobs.append(j)
            inactive_by_status[status_lbl or "(blank)"] += 1

    active_user_ids = {j.get("userId") for j in active_jobs if j.get("userId")}
    inactive_user_ids = {j.get("userId") for j in inactive_jobs if j.get("userId")}

    # ------------------------------------------------------------
    # 2) Users - email hygiene (apply to ACTIVE employee population)
    # ------------------------------------------------------------
    users = sf.get_all("/odata/v2/User", {"$select": "userId,email,username,status"})
    user_map = {u.get("userId"): u for u in users if u.get("userId")}

    active_users = [user_map[uid] for uid in active_user_ids if uid in user_map]
    total_active = len(active_users)

    missing_email_sample = []
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

    missing_email_count = sum(
        1 for u in active_users if is_missing_email_value(u.get("email"))
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

    # ------------------------------------------------------------
    # 3) Org + Manager gates (do checks on ACTIVE jobs only)
    # ------------------------------------------------------------
    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    for j in active_jobs:
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

    # ------------------------------------------------------------
    # 4) Inactive users sample (based on EmpJob employee status)
    # ------------------------------------------------------------
    inactive_users_sample = []
    for j in inactive_jobs[:MAX_SAMPLE]:
        uid = j.get("userId")
        u = user_map.get(uid, {})
        inactive_users_sample.append(
            {
                "userId": uid,
                "employeeStatus": job_status_label(j) or "(blank)",
                "username": u.get("username"),
                "email": u.get("email"),
            }
        )

    inactive_user_count = len(inactive_user_ids)

    # ------------------------------------------------------------
    # 5) Contingent workers (EmpEmployment.isContingentWorker)
    # ------------------------------------------------------------
    contingent_workers_sample = []
    contingent_user_ids = set()
    contingent_source = "EmpEmployment.isContingentWorker"

    try:
        cont_rows = sf.get_all(
            "/odata/v2/EmpEmployment",
            {
                "$select": "userId,isContingentWorker",
                "$filter": "isContingentWorker eq true",
            },
        )
        for r in cont_rows:
            uid = r.get("userId")
            if uid:
                contingent_user_ids.add(uid)

        # Build sample with employee status if we have it
        # (Status comes from EmpJob; if no job row found, leave blank)
        job_by_uid = {j.get("userId"): j for j in jobs if j.get("userId")}
        for uid in list(contingent_user_ids)[:MAX_SAMPLE]:
            u = user_map.get(uid, {})
            j = job_by_uid.get(uid, {})
            contingent_workers_sample.append(
                {
                    "userId": uid,
                    "isContingentWorker": True,
                    "employeeStatus": job_status_label(j) or "",
                    "username": u.get("username"),
                    "email": u.get("email"),
                }
            )

    except Exception:
        # Fallback (older logic) if EmpEmployment is not readable
        contingent_source = "fallback: EmpJob employeeClass/employeeType/employmentType (best-effort)"
        # Try to refetch extra fields safely
        extra_select = (
            "userId,effectiveLatestChange,employeeClass,employeeType,employmentType"
        )
        try:
            jobs2 = sf.get_all(
                "/odata/v2/EmpJob",
                {"$select": extra_select, "$filter": "effectiveLatestChange eq true"},
            )
        except Exception:
            jobs2 = []

        def is_contingent_job(j: dict) -> bool:
            raw = j.get("employeeClass") or j.get("employmentType") or j.get("employeeType") or ""
            s = _norm_status(raw)
            return bool(s) and (
                s in ("c", "contingent", "contingent worker", "contractor")
                or ("conting" in s)
                or ("contract" in s)
            )

        for j in jobs2:
            if is_contingent_job(j):
                uid = j.get("userId")
                if uid:
                    contingent_user_ids.add(uid)

        for uid in list(contingent_user_ids)[:MAX_SAMPLE]:
            u = user_map.get(uid, {})
            contingent_workers_sample.append(
                {
                    "userId": uid,
                    "isContingentWorker": True,
                    "username": u.get("username"),
                    "email": u.get("email"),
                }
            )

    contingent_worker_count = len(contingent_user_ids)
    contingent_active_count = len(contingent_user_ids.intersection(active_user_ids))

    # ------------------------------------------------------------
    # 6) Percent + Risk score (use ACTIVE employee count)
    # ------------------------------------------------------------
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

    # ------------------------------------------------------------
    # OUTPUT (keys Streamlit expects)
    # ------------------------------------------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # KPIs
        "active_users": total_active,
        "empjob_rows": len(jobs),              # all latest jobs
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        # Inactive (based on EmpJob employee status)
        "inactive_users": inactive_user_count,
        "inactive_user_count": inactive_user_count,
        "inactive_users_by_status": dict(inactive_by_status),

        # Contingent (based on EmpEmployment.isContingentWorker)
        "contingent_workers": contingent_worker_count,
        "contingent_worker_count": contingent_worker_count,
        "contingent_workers_active": contingent_active_count,
        "contingent_source": contingent_source,

        # Drilldowns
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,

        # Metadata (helps debugging without showing code)
        "employee_status_source": empl_status_source,
    }

    return metrics
