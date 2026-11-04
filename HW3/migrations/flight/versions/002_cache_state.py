from alembic import op

revision = "flight_002"
down_revision = "flight_001"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE cache_state (
            id integer PRIMARY KEY CHECK (id = 1),
            version bigint NOT NULL CHECK (version >= 0)
        );
        INSERT INTO cache_state (id, version) VALUES (1, 0);
    """)


def downgrade():
    op.execute("DROP TABLE cache_state")
