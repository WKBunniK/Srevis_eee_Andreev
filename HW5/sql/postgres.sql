CREATE TABLE IF NOT EXISTS daily_metrics (
    date date NOT NULL,
    metric_name text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (date, metric_name, dimension)
);
