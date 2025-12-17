from datetime import datetime, timezone
from collections import defaultdict, Counter


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def is_missing_email_value(v) -> bool:
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "none", "no_email", "no email", "null", "n/a", "na", "-", "undefined")


def _status_display(raw) -> str:
    """
    EmpJob.emplStatus is often a CODE (e.g., 'A') not the LABEL ('Active').
    We normalize to something human-friendly where possible.
    """
    if raw is None:
        return ""
    s = str(raw).strip()

    # Common code patterns
    if s.upper() == "A":
        return "Active"

    # If it's already a label
    if s.lower() == "active":
        return "Active"

    return s  # keep as-is (e.g., Terminated/Retired/etc. if tenant returns labels)


def _is_active_status(raw) -> bool:
    s = str(raw or "").strip().lower()
    return s in ("a", "active")  # treat only Active as active (matches your Excel logic)


def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)

    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # EMPJOB (Latest records) -> drives ACTIVE/INACTIVE like your Excel
    # ---------------------------
    jobs = sf.get_all(
        "/odata/v2/EmpJob",
        {
            "$select": (
                "userId,managerId,company,businessUnit,division,department,location,"
                "effectiveLatestChange,emplStatus"
            ),
            "$filter": "effectiveLatestChange eq true",
        },
    )

    # Build active/inactive sets from EmpJob.emplStatus
    active_job_user_ids = set()
    inactive_job_user_ids = set()
    inactive_users_by_status = Counter()

    # Also keep a distribution for debugging
    empl_status_dist = Counter()

    for j in jobs:
        uid = j.get("userId")
        raw_status = j.get("emplStatus")
        empl_status_dist[_status_display(raw_status)] += 1

        if _is_active_status(raw_status):
            if uid:
                active_job_user_ids.add(uid)
        else:
            if uid:
                inactive_job_user_ids.add(uid)
            inactive_users_by_status[_status_display(raw_status) or "(Blank)"] += 1

    active_jobs = [j for j in jobs if j.get("userId") in active_job_user_ids]

    # ---------------------------
    # USERS (for email hygiene + samples) — filter by ACTIVE employee status
    # ---------------------------
    users = sf.get_all(
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
    )

    user_by_id = {u.get("userId"): u for u in users if u.get("userId")}
    active_users = [user_by_id[uid] for uid in active_job_user_ids if uid in user_by_id]
    inactive_users_sample = []

    # sample inactive users (based on EmpJob status)
    for uid in list(inactive_job_user_ids)[:MAX_SAMPLE]:
        u = user_by_id.get(uid, {})
        inactive_users_sample.append(
            {
                "userId": uid,
                "username": u.get("username"),
                "email": u.get("email"),
            }
        )

    total_active = len(active_users)
    inactive_user_count = len(inactive_job_user_ids)

    # ---------------------------
    # Email hygiene (ACTIVE employees only)
    # ---------------------------
    missing_email_sample = []
    email_to_users = defaultdict(list)

    for u in active_users:
        uid = u.get("userId")
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        email_norm = email.lower()

        if is_missing_email_value(email):
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append({"userId": uid, "email": email, "username": u.get("username")})
            continue

        email_to_users[email_norm].append(uid)

    missing_email_count = sum(1 for u in active_users if is_missing_email_value(u.get("email")))

    duplicate_email_count = sum(
        (len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1
    )

    dup_rows = []
    for email, uids in email_to_users.items():
        if len(uids) > 1:
            dup_rows.append({"email": email, "count": len(uids), "sampleUserIds": uids[:MAX_USERS_PER_DUP_EMAIL]})
    dup_rows.sort(key=lambda x: x["count"], reverse=True)
    duplicate_email_sample = dup_rows[:MAX_SAMPLE]

    # ---------------------------
    # Org + manager checks (ACTIVE jobs only)
    # ---------------------------
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

    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)
    missing_email_pct = pct(missing_email_count)

    # ---------------------------
    # Contingent workers (EmpEmployment.isContingentWorker)
    # ---------------------------
    contingent_workers_sample = []
    try:
        cont = sf.get_all(
            "/odata/v2/EmpEmployment",
            {"$select": "userId,isContingentWorker", "$filter": "isContingentWorker eq true"},
        )
        contingent_user_ids = list({c.get("userId") for c in cont if c.get("userId")})
        contingent_worker_count = len(contingent_user_ids)
        contingent_workers_active = len([uid for uid in contingent_user_ids if uid in active_job_user_ids])

        for uid in contingent_user_ids[:MAX_SAMPLE]:
            contingent_workers_sample.append({"userId": uid})
        contingent_source = "EmpEmployment.isContingentWorker"
    except Exception:
        contingent_worker_count = 0
        contingent_workers_active = 0
        contingent_source = "not-available"

    # ---------------------------
    # Risk score
    # ---------------------------
    risk = 0
    risk += min(40, int(missing_manager_pct * 2))
    risk += min(40, int(invalid_org_pct * 2))
    risk += min(10, int(missing_email_pct))
    risk += min(10, int((duplicate_email_count / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    return {
        "snapshot_time_utc": now.isoformat(),

        "employee_status_source": "EmpJob.emplStatus",
        "active_users": total_active,
        "inactive_users": inactive_user_count,
        "inactive_users_by_status": dict(inactive_users_by_status),

        "empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,
        "missing_manager_sample": missing_manager_sample,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,
        "invalid_org_sample": invalid_org_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_count": missing_email_count,
        "missing_email_sample": missing_email_sample,
        "duplicate_email_count": duplicate_email_count,
        "duplicate_email_sample": duplicate_email_sample,

        "contingent_source": contingent_source,
        "contingent_workers": contingent_worker_count,
        "contingent_workers_active": contingent_workers_active,
        "contingent_workers_sample": contingent_workers_sample,

        "risk_score": risk_score,

        # optional debug distribution (only visible if you enable raw JSON)
        "employee_status_dist": dict(empl_status_dist),
    }
