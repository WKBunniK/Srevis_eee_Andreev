from alembic import op
revision = "booking_001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
    CREATE TABLE bookings (
        id uuid PRIMARY KEY,
        user_id varchar(100) NOT NULL CHECK (length(trim(user_id)) > 0),
        flight_id bigint NOT NULL CHECK (flight_id > 0),
        passenger_name varchar(200) NOT NULL CHECK (length(trim(passenger_name)) > 0),
        passenger_email varchar(254) NOT NULL CHECK (position('@' in passenger_email) > 1),
        seat_count integer NOT NULL CHECK (seat_count > 0),
        total_price_minor bigint NOT NULL CHECK (total_price_minor > 0),
        status varchar(10) NOT NULL CHECK (status IN ('CONFIRMED','CANCELLED')),
        created_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX booking_user ON bookings(user_id, created_at);
    """)


def downgrade():
    op.execute("DROP TABLE bookings")
