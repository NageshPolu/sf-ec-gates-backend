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

    ORG_FIELDS = ["company", "businessUnit", "division", "department", "location"]
    missing_manager = 0
    invalid_org = 0

    # Drilldown samples (keep small to avoid huge payloads)
    MAX_SAMPLE = 200
    invalid_org_rows = []
    missing_manager_rows = []
    org_missing_counts = {k: 0 for k in ORG_FIELDS}

    for j in jobs:
        uid = j.get("userId")
        mgr = j.get("managerId")

        # Missing manager
        if is_blank(mgr):
            missing
