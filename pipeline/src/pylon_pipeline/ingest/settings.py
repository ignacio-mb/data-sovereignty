"""Constants for the Pylon ingestion job."""

API_URL = "https://api.usepylon.com"

BACKFILL_START = "2019-01-01"

# The real warehouse. Any other destination is a smoke test and gets its own
# dlt pipeline name so it can never advance the production incremental cursor.
PRODUCTION_DESTINATION = "postgres"
# Schema the raw tables land in, inside the warehouse database.
DATASET_NAME = "raw_pylon"

# Requests per minute per endpoint family, from https://docs.usepylon.com.
# GET /accounts, /users, /teams and /contacts each document 60/min; they share
# one pacer family and are fetched sequentially, so pacing at 60 stays within
# each endpoint's own budget.
RATE_LIMITS = {
    "issues_list": 10,
    "issues_search": 20,
    "messages": 20,
    "directory": 60,
}

# GET /issues requires start_time/end_time and caps the window at 30 days.
ISSUES_WINDOW_DAYS = 30
# GET /issues accepts limit up to 20000.
ISSUES_LIST_PAGE_LIMIT = 20000
# POST /issues/search caps limit below 1000; 500 keeps a safety margin.
ISSUES_SEARCH_PAGE_LIMIT = 500
DIRECTORY_PAGE_LIMIT = 999

DIRECTORY_RESOURCES = ("accounts", "users", "teams", "contacts")

# Tables that get the _deleted reconciliation pass. issue_messages is excluded:
# messages are only ever appended, and the per-issue fetch pattern means a full
# "absence" scan is never available for them.
SOFT_DELETE_DIRECTORY_TABLES = ("accounts", "users", "teams", "contacts")

# Overlap re-fetched on every incremental run to absorb late/out-of-order
# updated_at stamps. Merge on id makes the overlap idempotent.
INCREMENTAL_LOOKBACK_SECONDS = 3600

# Messages whose issue has latest_message_time within this many seconds of the
# already-loaded max(timestamp) are considered up to date (clock fudge).
MESSAGE_WATERMARK_FUDGE_SECONDS = 3

# Default wall-clock budget for the per-issue messages fetch in incremental
# runs. The run ends cleanly when exhausted; the watermark resumes next run.
MESSAGES_BUDGET_MINUTES_DEFAULT = 25
