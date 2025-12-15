from datetime import datetime, timezone

def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""

def run_ec_gates(sf) -> dict:
    now = datetime.now(timezone.utc)

    # USERS (email hygiene)
    users = sf.get_all("/odata/v2/User", {"$select": "userId,status,email,username"})

    def is_active_user(u: dict) -> bool:
        s = str(u.get("status", "")).lower()
        return s in ("active", "t", "true", "1")

    active_users = [u for u in users if is_active_user(u)]
    total_active = len(active_users)

    emails = [str(u.get("email", "")).strip().lower() for u in active_users if u.get("email")]
    missing_email = sum(1 for u in active_users if not str(u.get("email", "")).strip())
    dup_email = len(emails) - len(set(emails))

    # EMPJOB (your tenant confirmed fields)
    jobs = sf.get_all("/odata/v2/EmpJob", {
        "$select": "userId,managerId,company,businessUnit,division,department,location,effectiveLatestChange",
        "$filter": "effectiveLatestChange eq true",
    })

    missing_manager = 0
    invalid_org = 0

    for j in jobs:
        if is_blank(j.get("managerId")):
            missing_manager += 1
        if any(is_blank(j.get(k)) for k in ["company","businessUnit","division","department","location"]):
            invalid_org += 1

    def pct(x: int) -> float:
        return 0.0 if total_active == 0 else round((x / total_active) * 100, 2)

    metrics = {
        "snapshot_time_utc": now.isoformat(),
        "active_users": total_active,
        "current_empjob_rows": len(jobs),

        "missing_manager_count": missing_manager,
        "missing_manager_pct": pct(missing_manager),

        "invalid_org_count": invalid_org,
        "invalid_org_pct": pct(invalid_org),

        "missing_email_count": missing_email,
        "duplicate_email_count": dup_email,
    }

    # Simple risk score (0–100)
    risk = 0
    risk += min(40, int(metrics["missing_manager_pct"] * 2))
    risk += min(40, int(metrics["invalid_org_pct"] * 2))
    risk += min(10, int((missing_email / max(1, total_active)) * 100))
    risk += min(10, int((dup_email / max(1, total_active)) * 100))
    metrics["risk_score"] = min(100, risk)

    return metrics
