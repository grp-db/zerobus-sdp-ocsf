"""
SDP Gold Layer — OCSF-normalized streaming tables (Delta sinks).

Maps the flattened silver GitHub events to three OCSF v1.7.0 event classes:

  1. API Activity         (class_uid 6003)  — all GitHub events as API calls
  2. Account Change       (class_uid 3001)  — MemberEvent → user/role changes
  3. File System Activity (class_uid 1001)  — PushEvent   → file modifications

Each gold table is a streaming table with an append flow that reads from
the silver streaming table and writes OCSF-normalized records into the
pre-created Delta sinks (see ddl_ocsf.py).
"""

from pyspark import pipelines as sdp

from utils_pipeline import TABLES, FQN, OCSF

# ═══════════════════════════════════════════════════════════════════════════
# Reusable OCSF metadata struct expression
# ═══════════════════════════════════════════════════════════════════════════

METADATA_EXPR = f"""named_struct(
    'version',        '{OCSF["version"]}',
    'product',        named_struct(
                          'name',        'GitHub',
                          'vendor_name', 'GitHub, Inc.',
                          'version',     '2022-11-28'
                      ),
    'log_provider',   source,
    'log_name',       source_type,
    'log_format',     'JSON',
    'log_version',    'github@events:version@1.0',
    'processed_time', current_timestamp(),
    'logged_time',    event_time
) AS metadata"""


# ═══════════════════════════════════════════════════════════════════════════
# GitHub event type → OCSF activity mapping helpers
# ═══════════════════════════════════════════════════════════════════════════

_API_ACTIVITY_MAP = {
    "CreateEvent":             "1",   # Create
    "PushEvent":               "3",   # Update
    "DeleteEvent":             "4",   # Delete
    "PullRequestEvent":        "3",   # Update
    "PullRequestReviewEvent":  "2",   # Read
    "IssuesEvent":             "3",   # Update
    "IssueCommentEvent":       "1",   # Create
    "WatchEvent":              "2",   # Read
    "ForkEvent":               "1",   # Create
    "ReleaseEvent":            "1",   # Create
    "MemberEvent":             "3",   # Update
    "PublicEvent":             "3",   # Update
    "GollumEvent":             "3",   # Update
    "CommitCommentEvent":      "1",   # Create
}

_API_ACTIVITY_NAME_MAP = {
    "1": "Create",
    "2": "Read",
    "3": "Update",
    "4": "Delete",
}


def _build_case_expr(mapping: dict[str, str], input_col: str, default: str) -> str:
    """Build a SQL CASE WHEN expression from a mapping dict."""
    clauses = " ".join(
        f"WHEN {input_col} = '{k}' THEN '{v}'" for k, v in mapping.items()
    )
    return f"CASE {clauses} ELSE '{default}' END"


# ═══════════════════════════════════════════════════════════════════════════
# 1) API Activity  (class_uid 6003) — Delta sink
# ═══════════════════════════════════════════════════════════════════════════

sdp.create_sink(
    name="api_activity",
    format="delta",
    options={"tableName": FQN["api_activity"]},
)

@sdp.append_flow(target="api_activity")
def api_activity_flow():
    activity_id_expr = _build_case_expr(_API_ACTIVITY_MAP, "event_type", "99")
    activity_name_expr = _build_case_expr(
        {v: _API_ACTIVITY_NAME_MAP.get(v, "Other") for v in set(_API_ACTIVITY_MAP.values())},
        f"({activity_id_expr})",
        "Other",
    )

    return (
        spark.readStream
        .table(TABLES["silver"])
        .selectExpr(
            # ── classification ──
            f"{OCSF['class']['api_activity']}                   AS class_uid",
            "'API Activity'                                     AS class_name",
            f"{OCSF['category']['application']}                 AS category_uid",
            "'Application Activity'                             AS category_name",
            f"CAST({activity_id_expr} AS INT)                   AS activity_id",
            f"{activity_name_expr}                              AS activity_name",
            f"CAST({OCSF['class']['api_activity']} * 100 + CAST({activity_id_expr} AS INT) AS BIGINT) AS type_uid",
            f"CONCAT('API Activity: ', {activity_name_expr})    AS type_name",
            "1                                                  AS severity_id",
            "'Informational'                                    AS severity",

            # ── occurrence ──
            "event_time                                         AS time",
            "event_date",

            # ── status ──
            "'Success'                                          AS status",
            "1                                                  AS status_id",

            # ── api object ──
            "named_struct('operation', event_type, 'service', named_struct('name', 'GitHub')) AS api",

            # ── actor ──
            "named_struct('user', named_struct('name', actor_login, 'uid', CAST(actor_id AS STRING), 'url', actor_url)) AS actor",

            # ── source endpoint ──
            "named_struct('url', actor_url) AS src_endpoint",

            # ── resources ──
            "named_struct('name', repo_name, 'uid', CAST(repo_id AS STRING), 'url', repo_url) AS resources",

            # ── message ──
            "CONCAT(actor_login, ' performed ', event_type, ' on ', repo_name) AS message",

            # ── metadata (OCSF struct) ──
            METADATA_EXPR,

            # ── raw data (full event payload) ──
            "CAST(to_json(named_struct('payload', data, 'source', source, 'sourcetype', source_type)) AS VARIANT) AS raw_data",

            # ── unmapped (single VARIANT) ──
            """parse_json(to_json(named_struct(
                'payload_action', payload_action,
                'org_login',      org_login,
                'event_id',       event_id
            )))                                                 AS unmapped""",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2) Account Change  (class_uid 3001) — Delta sink
# ═══════════════════════════════════════════════════════════════════════════

sdp.create_sink(
    name="account_change",
    format="delta",
    options={"tableName": FQN["account_change"]},
)

@sdp.append_flow(target="account_change")
def account_change_flow():
    return (
        spark.readStream
        .table(TABLES["silver"])
        .where("event_type IN ('MemberEvent', 'WatchEvent', 'ForkEvent')")
        .selectExpr(
            # ── classification ──
            f"{OCSF['class']['account_change']}                 AS class_uid",
            "'Account Change'                                   AS class_name",
            f"{OCSF['category']['iam']}                         AS category_uid",
            "'Identity & Access Management'                     AS category_name",
            """CASE
                WHEN event_type = 'MemberEvent' AND payload_action = 'added'   THEN 1
                WHEN event_type = 'MemberEvent' AND payload_action = 'removed' THEN 6
                WHEN event_type = 'WatchEvent'  THEN 1
                WHEN event_type = 'ForkEvent'   THEN 1
                ELSE 99
            END                                                 AS activity_id""",
            """CASE
                WHEN event_type = 'MemberEvent' AND payload_action = 'added'   THEN 'Create'
                WHEN event_type = 'MemberEvent' AND payload_action = 'removed' THEN 'Delete'
                WHEN event_type = 'WatchEvent'  THEN 'Create'
                WHEN event_type = 'ForkEvent'   THEN 'Create'
                ELSE 'Other'
            END                                                 AS activity_name""",
            f"""CAST({OCSF['class']['account_change']} * 100 +
                CASE
                    WHEN event_type = 'MemberEvent' AND payload_action = 'added'   THEN 1
                    WHEN event_type = 'MemberEvent' AND payload_action = 'removed' THEN 6
                    WHEN event_type = 'WatchEvent'  THEN 1
                    WHEN event_type = 'ForkEvent'   THEN 1
                    ELSE 99
                END AS BIGINT)                                  AS type_uid""",
            """CONCAT('Account Change: ',
                CASE
                    WHEN event_type = 'MemberEvent' AND payload_action = 'added'   THEN 'Create'
                    WHEN event_type = 'MemberEvent' AND payload_action = 'removed' THEN 'Delete'
                    WHEN event_type = 'WatchEvent'  THEN 'Create'
                    WHEN event_type = 'ForkEvent'   THEN 'Create'
                    ELSE 'Other'
                END)                                            AS type_name""",
            "1                                                  AS severity_id",
            "'Informational'                                    AS severity",

            # ── occurrence ──
            "event_time                                         AS time",
            "event_date",

            # ── status ──
            "'Success'                                          AS status",
            "1                                                  AS status_id",

            # ── user (target of the change — member for MemberEvent, actor for Watch/Fork) ──
            """named_struct(
                'name', CASE WHEN event_type = 'MemberEvent' THEN member_login ELSE actor_login END,
                'uid',  CASE WHEN event_type = 'MemberEvent' THEN CAST(member_id AS STRING) ELSE CAST(actor_id AS STRING) END,
                'type', CASE WHEN event_type = 'MemberEvent' THEN member_type ELSE 'User' END
            ) AS user""",

            # ── actor (who performed the change) ──
            "named_struct('user', named_struct('name', actor_login, 'uid', CAST(actor_id AS STRING), 'url', actor_url)) AS actor",

            # ── source endpoint ──
            "named_struct('url', actor_url) AS src_endpoint",

            # ── message ──
            """CASE
                WHEN event_type = 'MemberEvent' THEN CONCAT(actor_login, ' ', payload_action, ' member ', member_login, ' on ', repo_name)
                WHEN event_type = 'WatchEvent'  THEN CONCAT(actor_login, ' starred ', repo_name)
                WHEN event_type = 'ForkEvent'   THEN CONCAT(actor_login, ' forked ', repo_name)
                ELSE CONCAT(actor_login, ' performed ', event_type, ' on ', repo_name)
            END                                                 AS message""",

            # ── metadata (OCSF struct) ──
            METADATA_EXPR,

            # ── raw data (full event payload) ──
            "CAST(to_json(named_struct('payload', data, 'source', source, 'sourcetype', source_type)) AS VARIANT) AS raw_data",

            # ── unmapped (single VARIANT) ──
            """parse_json(to_json(named_struct(
                'repo_name',  repo_name,
                'repo_id',    CAST(repo_id AS STRING),
                'org_login',  org_login,
                'event_id',   event_id
            )))                                                 AS unmapped""",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 3) File System Activity  (class_uid 1001) — Delta sink
# ═══════════════════════════════════════════════════════════════════════════

sdp.create_sink(
    name="file_system_activity",
    format="delta",
    options={"tableName": FQN["file_system_activity"]},
)

@sdp.append_flow(target="file_system_activity")
def file_system_activity_flow():
    return (
        spark.readStream
        .table(TABLES["silver"])
        .where("event_type = 'PushEvent'")
        .selectExpr(
            # ── classification ──
            f"{OCSF['class']['file_system_activity']}           AS class_uid",
            "'File System Activity'                             AS class_name",
            f"{OCSF['category']['system']}                     AS category_uid",
            "'System Activity'                                  AS category_name",
            "3                                                  AS activity_id",
            "'Update'                                           AS activity_name",
            f"CAST({OCSF['class']['file_system_activity'] * 100 + 3} AS BIGINT) AS type_uid",
            "'File System Activity: Update'                     AS type_name",
            "1                                                  AS severity_id",
            "'Informational'                                    AS severity",

            # ── occurrence ──
            "event_time                                         AS time",
            "event_date",

            # ── status ──
            "'Success'                                          AS status",
            "1                                                  AS status_id",

            # ── file object (branch ref as the file path) ──
            "named_struct('name', payload_ref, 'path', CONCAT(repo_name, '/', COALESCE(payload_ref, ''))) AS file",

            # ── actor ──
            "named_struct('user', named_struct('name', actor_login, 'uid', CAST(actor_id AS STRING), 'url', actor_url)) AS actor",

            # ── source endpoint ──
            "named_struct('url', actor_url) AS src_endpoint",

            # ── message ──
            "CONCAT(actor_login, ' pushed ', "
            "COALESCE(CAST(payload_distinct_size AS STRING), '0'), "
            "' commit(s) to ', payload_ref, ' in ', repo_name) AS message",

            # ── metadata (OCSF struct) ──
            METADATA_EXPR,

            # ── raw data (full event payload) ──
            "CAST(to_json(named_struct('payload', data, 'source', source, 'sourcetype', source_type)) AS VARIANT) AS raw_data",

            # ── unmapped (single VARIANT) ──
            """parse_json(to_json(named_struct(
                'head_sha',       payload_head_sha,
                'before_sha',     payload_before_sha,
                'push_size',      CAST(payload_size AS STRING),
                'distinct_size',  CAST(payload_distinct_size AS STRING),
                'commits_json',   payload_commits_json,
                'repo_id',        CAST(repo_id AS STRING),
                'org_login',      org_login,
                'event_id',       event_id
            )))                                                 AS unmapped""",
        )
    )
