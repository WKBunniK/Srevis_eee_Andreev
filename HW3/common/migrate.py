import os
import sys
from pathlib import Path
from alembic import command
from alembic.config import Config


def upgrade(service):
    if service not in {"booking", "flight"}:
        raise ValueError("Unknown service")
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).resolve().parent.parent / "migrations" / service))

    url = os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+psycopg://", 1)
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.upgrade(config, "head")


if __name__ == "__main__":
    upgrade(sys.argv[1])
