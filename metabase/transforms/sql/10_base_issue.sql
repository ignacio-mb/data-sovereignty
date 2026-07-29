-- base_issue — one row per Pylon issue, every JSON blob decoded.
--
-- raw_pylon.issues keeps nested JSON stringified into String columns on
-- purpose (a slug-keyed custom_fields map would otherwise mint a new column
-- per support config change). This is where that trade is paid back: the five
-- JSON columns become typed, named, filterable ones.
--
--   account / requester / assignee   id-only objects; the id is already
--                                    promoted at ingest, so all this adds is
--                                    the human label from the lookup tables
--   custom_fields                    {slug: {value | values}} — this tenant
--                                    populates `priority` and `question_type`
--   tags                             free-form string array
--   time_in_status_seconds           {status: seconds}, and its business-hours
--   (+ business hours variant)       twin — pivoted to one column per status
--
-- Multi-value fields are kept as both an Array(String) (exact matching in SQL)
-- and a delimited label (grouping in the Metabase UI, which cannot filter an
-- array). Per-tag breakouts need `ARRAY JOIN tags`; a bridge table is the fix
-- if that becomes a routine question.

SELECT
    -- ── identity ────────────────────────────────────────────────────────────
    i.id                                                                    AS issue_id,
    i.number                                                                AS issue_number,
    i.title,
    i.link                                                                  AS issue_url,
    nullIf(extractTextFromHTML(ifNull(i.body_html, '')), '')                AS body_text,

    -- ── classification ──────────────────────────────────────────────────────
    i.source,
    i.type                                                                  AS issue_type,
    i.state,
    CASE i.state
        WHEN 'closed'              THEN 'closed'
        WHEN 'new'                 THEN 'awaiting_us'
        WHEN 'waiting_on_you'      THEN 'awaiting_us'
        WHEN 'waiting_on_customer' THEN 'awaiting_customer'
        WHEN 'on_hold'             THEN 'on_hold'
        ELSE 'unknown'
    END                                                                     AS state_bucket,
    toBool(i.state != 'closed')                                             AS is_open,
    toBool(i.resolution_time IS NOT NULL)                                   AS is_resolved,

    -- ── custom fields ───────────────────────────────────────────────────────
    nullIf(JSONExtractString(ifNull(i.custom_fields, ''), 'priority', 'value'), '')
                                                                            AS priority,
    -- Ordinal so severity can be averaged, sorted and thresholded. Pylon
    -- stores the slug only; the ranking is ours.
    CASE JSONExtractString(ifNull(i.custom_fields, ''), 'priority', 'value')
        WHEN 'urgent' THEN 4
        WHEN 'high'   THEN 3
        WHEN 'medium' THEN 2
        WHEN 'low'    THEN 1
        ELSE NULL
    END                                                                     AS priority_rank,
    arraySort(JSONExtract(ifNull(i.custom_fields, ''), 'question_type', 'values', 'Array(String)'))
                                                                            AS question_types,
    nullIf(arrayStringConcat(question_types, ', '), '')                     AS question_type_label,
    length(question_types)                                                  AS question_type_count,
    -- One boolean per configured option: Metabase cannot filter an array, and
    -- these four are field configuration in Pylon, not free-form data. A new
    -- option shows up in question_type_label until a column is added for it.
    toBool(has(question_types, 'bug'))                                      AS is_bug,
    toBool(has(question_types, 'feature_request'))                          AS is_feature_request,
    toBool(has(question_types, 'user_error'))                               AS is_user_error,
    toBool(has(question_types, 'meeting_scheduling'))                       AS is_meeting_scheduling,

    -- ── tags ────────────────────────────────────────────────────────────────
    arraySort(JSONExtract(ifNull(i.tags, ''), 'Array(String)'))             AS tags,
    nullIf(arrayStringConcat(tags, ', '), '')                               AS tags_label,
    length(tags)                                                            AS tag_count,

    -- ── lifecycle ───────────────────────────────────────────────────────────
    -- Explicitly aliased: `accounts` also has created_at/updated_at, and
    -- ClickHouse keeps the `i.` qualifier in the output name when a bare
    -- column is ambiguous across the joined tables.
    i.created_at                                                            AS created_at,
    i.updated_at                                                            AS updated_at,
    i.resolution_time                                                       AS resolved_at,
    i.latest_message_time                                                   AS last_message_at,

    i.resolution_seconds,
    round(i.resolution_seconds / 3600, 3)                                   AS resolution_hours,
    i.business_hours_resolution_seconds,
    round(i.business_hours_resolution_seconds / 3600, 3)                    AS business_hours_resolution_hours,

    -- A key absent from the map means the issue never entered that status, so
    -- JSONExtractInt's 0 is the right answer. An absent *map* means Pylon
    -- reported no status history at all, which is not the same thing and
    -- stays NULL.
    if(i.time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.time_in_status_seconds), 'open'))     AS seconds_in_open,
    if(i.time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.time_in_status_seconds), 'in_progress'))
                                                                            AS seconds_in_in_progress,
    if(i.time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.time_in_status_seconds), 'on_hold'))  AS seconds_in_on_hold,
    if(i.time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.time_in_status_seconds), 'waiting_on_action'))
                                                                            AS seconds_in_waiting_on_action,
    if(i.business_hours_time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.business_hours_time_in_status_seconds), 'open'))
                                                                            AS business_seconds_in_open,
    if(i.business_hours_time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.business_hours_time_in_status_seconds), 'in_progress'))
                                                                            AS business_seconds_in_in_progress,
    if(i.business_hours_time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.business_hours_time_in_status_seconds), 'on_hold'))
                                                                            AS business_seconds_in_on_hold,
    if(i.business_hours_time_in_status_seconds IS NULL, NULL,
       JSONExtractInt(assumeNotNull(i.business_hours_time_in_status_seconds), 'waiting_on_action'))
                                                                            AS business_seconds_in_waiting_on_action,
    -- Summed over every key in the map rather than over the four columns
    -- above, so the manifest reconciliation catches a status key Pylon starts
    -- reporting that we never pivoted.
    if(i.time_in_status_seconds IS NULL, NULL,
       arraySum(mapValues(JSONExtract(assumeNotNull(i.time_in_status_seconds), 'Map(String, Int64)'))))
                                                                            AS tracked_status_seconds,
    if(i.business_hours_time_in_status_seconds IS NULL, NULL,
       arraySum(mapValues(JSONExtract(assumeNotNull(i.business_hours_time_in_status_seconds),
                                      'Map(String, Int64)'))))
                                                                            AS tracked_business_status_seconds,

    -- ── account ─────────────────────────────────────────────────────────────
    i.account_id,
    account.name                                                            AS account_name,
    coalesce(account.primary_domain, account.domain)                        AS account_domain,
    account.type                                                            AS account_type,
    ifNull(account.is_disabled, false)                                      AS account_is_disabled,
    arraySort(JSONExtract(ifNull(account.tags, ''), 'Array(String)'))       AS account_tags,
    nullIf(arrayStringConcat(account_tags, ', '), '')                       AS account_tags_label,

    -- ── people ──────────────────────────────────────────────────────────────
    i.assignee_id,
    assignee.name                                                           AS assignee_name,
    assignee.email                                                          AS assignee_email,
    toBool(i.assignee_id IS NOT NULL)                                       AS is_assigned,
    i.requester_id,
    requester.name                                                          AS requester_name,
    requester.email                                                         AS requester_email,
    requester.portal_role                                                   AS requester_portal_role,

    -- ── remaining flags ─────────────────────────────────────────────────────
    ifNull(i.customer_portal_visible, false)                                AS customer_portal_visible,
    ifNull(i.author_unverified, false)                                      AS author_unverified,
    i.number_of_touches

FROM raw_pylon.issues AS i
-- Every lookup key is unique in its source table, so none of these can fan the
-- grain out. Unmatched rows land as NULL, which the manifest reconciles.
LEFT JOIN raw_pylon.accounts AS account   ON account.id = i.account_id
LEFT JOIN raw_pylon.users    AS assignee  ON assignee.id = i.assignee_id
LEFT JOIN raw_pylon.contacts AS requester ON requester.id = i.requester_id
-- Soft-deleted rows are tombstones from a reconcile run, not history.
WHERE NOT ifNull(i._deleted, false)
