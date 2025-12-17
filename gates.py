from datetime import datetime, timezone
from collections import defaultdict


def run_ec_gates(sf) -> dict:
    """
    EC Go-Live Gates snapshot with drilldowns (API-only).
    Data sources: OData v2 User + EmpJob (latest).
    """
    now = datetime.now(timezone.utc)

    # Keep payload safe
    MAX_SAMPLE = 200
    MAX_USERS_PER_DUP_EMAIL = 10

    # ---------------------------
    # USERS (Active + Inactive + email hygiene)
    # ---------------------------
    users = sf.get_all(
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
    )

    def is_active_user(u: dict) -> bool:
        s = str(u.get("status", "")).strip().lower()
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user(u)]
    inactive_users = [u for u in users if not is_active_user(u)]

    total_active = len(active_users)
    inactive_user_count = len(inactive_users)

    inactive_users_sample = []
    for u in inactive_users[:MAX_SAMPLE]:
        inactive_users_sample.append(
            {
                "userId": u.get("userId"),
                "status": u.get("status"),
                "email": u.get("email"),
                "username": u.get("username"),
            }
        )

    missing_email_sample = []
    email_to_users = defaultdict(list)

    for u in active_users:
        uid = u.get("userId")
        raw_email = u.get("email")
        email = "" if raw_email is None else str(raw_email).strip()
        email_norm = email.lower()

        # Missing email (blank OR placeholder)
        if is_missing_email_value(email):
            if len(missing_email_sample) < MAX_SAMPLE:
                missing_email_sample.append(
                    {"userId": uid, "email": email, "username": u.get("username")}
                )
            continue

        # Valid email -> count duplicates
        email_to_users[email_norm].append(uid)

    missing_email_count = sum(1 for u in active_users if is_missing_email_value(u.get("email")))

    duplicate_email_count = sum(
        (len(uids) - 1) for _, uids in email_to_users.items() if len(uids) > 1
    )

    # Build duplicate email sample (top by count)
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
    # EMPJOB (Latest records)
    # ---------------------------
    # We try to fetch extra fields that *might* exist to detect contingent workers.
    # If tenant doesn't support these fields, we fall back safely to the original select.
    base_select = (
        "userId,managerId,company,businessUnit,division,department,location,"
        "effectiveLatestChange"
    )
    extra_select = base_select + ",employeeClass,employeeType,employmentType"

    try:
        jobs = sf.get_all(
            "/odata/v2/EmpJob",
            {"$select": extra_select, "$filter": "effectiveLatestChange eq true"},
        )
        contingent_source = "EmpJob.employeeClass/employeeType/employmentType"
    except Exception:
        jobs = sf.get_all(
            "/odata/v2/EmpJob",
            {"$select": base_select, "$filter": "effectiveLatestChange eq true"},
        )
        contingent_source = "not-available (no EmpJob class fields)"

    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    # NEW: contingent workers
    contingent_worker_count = 0
    contingent_workers_sample = []

    def is_contingent_job(j: dict) -> bool:
        # Very tolerant matching:
        # - employeeClass often is "C" or "Contingent"
        # - employmentType/employeeType sometimes contain "contingent"
        raw = (
            j.get("employeeClass")
            or j.get("employmentType")
            or j.get("employeeType")
            or ""
        )
        s = str(raw).strip().lower()
        if not s:
            return False
        return s in ("c", "contingent", "contingent worker", "contractor") or ("conting" in s) or ("contract" in s)

    for j in jobs:
        uid = j.get("userId")
        mgr = j.get("managerId")

        # Missing manager
        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append({"userId": uid, "managerId": mgr})

        # Invalid org fields
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

        # Contingent workers (based on available EmpJob fields)
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

    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

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
    risk += min(10, int((duplicate_email_count / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    # ---------------------------
    # OUTPUT (include alias keys so Streamlit can read without changes)
    # ---------------------------
    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # KPIs (primary)
        "active_users": total_active,
        "empjob_rows": len(jobs),                  # <-- Streamlit expects this
        "current_empjob_rows": len(jobs),          # keep your existing key too

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email_count,
        "duplicate_email_count": duplicate_email_count,

        "risk_score": risk_score,

        # NEW KPIs
        "inactive_users": inactive_user_count,           # <-- Streamlit expects this
        "inactive_user_count": inactive_user_count,      # extra alias

        "contingent_workers": contingent_worker_count,        # <-- Streamlit expects this
        "contingent_worker_count": contingent_worker_count,   # extra alias
        "contingent_source": contingent_source,

        # Drilldowns
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,

        "missing_email_sample": missing_email_sample,
        "duplicate_email_sample": duplicate_email_sample,

        # NEW drilldowns
        "inactive_users_sample": inactive_users_sample,
        "contingent_workers_sample": contingent_workers_sample,
    }

    return metrics
