from datetime import datetime, timezone


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def run_ec_gates(sf) -> dict:
    """
    Builds a lightweight EC go-live snapshot with drilldowns.
    - Uses OData v2 User + EmpJob (latest) only
    - Returns summary metrics + sample offender rows
    """
    now = datetime.now(timezone.utc)

    # ---------------------------
    # USERS (Active count + email hygiene)
    # ---------------------------
    users = sf.get_all(
        "/odata/v2/User",
        {"$select": "userId,status,email,username"},
    )

    def is_active_user(u: dict) -> bool:
        s = str(u.get("status", "")).strip().lower()
        # SuccessFactors can return status in different formats depending on tenant/config
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user(u)]
    total_active = len(active_users)

    emails = [
        str(u.get("email", "")).strip().lower()
        for u in active_users
        if str(u.get("email", "")).strip()
    ]
    missing_email = sum(1 for u in active_users if is_blank(u.get("email")))
    duplicate_email = len(emails) - len(set(emails))

    # ---------------------------
    # EMPJOB (Latest records)
    # ---------------------------
    jobs = sf.get_all(
        "/odata/v2/EmpJob",
        {
            "$select": (
                "userId,managerId,company,businessUnit,division,department,location,"
                "effectiveLatestChange"
            ),
            "$filter": "effectiveLatestChange eq true",
        },
    )

    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]

    missing_manager_count = 0
    invalid_org_count = 0

    # Drilldown samples (limit to keep payload safe)
    MAX_SAMPLE = 200
    missing_manager_sample = []
    invalid_org_sample = []
    org_missing_field_counts = {k: 0 for k in ORG_FIELDS}

    for j in jobs:
        uid = j.get("userId")
        mgr = j.get("managerId")

        # Missing manager
        if is_blank(mgr):
            missing_manager_count += 1
            if len(missing_manager_sample) < MAX_SAMPLE:
                missing_manager_sample.append(
                    {
                        "userId": uid,
                        "managerId": mgr,
                    }
                )

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

    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    missing_manager_pct = pct(missing_manager_count)
    invalid_org_pct = pct(invalid_org_count)

    # ---------------------------
    # Risk score (simple, explainable)
    # ---------------------------
    risk = 0
    # Manager + org are dominant gates
    risk += min(40, int(missing_manager_pct * 2))  # 20% -> 40 points
    risk += min(40, int(invalid_org_pct * 2))      # 20% -> 40 points

    # Email hygiene minor
    risk += min(10, int((missing_email / max(1, total_active)) * 100))
    risk += min(10, int((duplicate_email / max(1, total_active)) * 100))
    risk_score = min(100, risk)

    metrics = {
        "snapshot_time_utc": now.isoformat(),

        # Top KPIs
        "active_users": total_active,
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager_count,
        "missing_manager_pct": missing_manager_pct,

        "invalid_org_count": invalid_org_count,
        "invalid_org_pct": invalid_org_pct,

        "missing_email_count": missing_email,
        "duplicate_email_count": duplicate_email,

        "risk_score": risk_score,

        # ✅ Drilldowns
        "invalid_org_sample": invalid_org_sample,
        "missing_manager_sample": missing_manager_sample,
        "org_missing_field_counts": org_missing_field_counts,
    }

    return metrics
