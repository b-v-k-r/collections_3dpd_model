import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    import load_dotenv as _load_dotenv_module

    load_dotenv = _load_dotenv_module.load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
DOTENV_PATH = PROJECT_ROOT / ".env"

if DOTENV_PATH.exists():
    load_dotenv(DOTENV_PATH)
else:
    load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "client_session_keep_alive": os.getenv(
        "SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE", "true"
    ).lower()
    == "true",
    "query_timeout": int(os.getenv("SNOWFLAKE_QUERY_TIMEOUT", "120")),
}
print(SNOWFLAKE_CONFIG)

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
CHECKPOINT_DIR = Path(
    os.getenv(
        "CHECKPOINT_DIR",
        PROJECT_ROOT
        / "features"
        / "mandates_features"
        / "checkpoints",
    )
)
CHECKPOINT_ENABLED = os.getenv("CHECKPOINT_ENABLED", "true").lower() == "true"
