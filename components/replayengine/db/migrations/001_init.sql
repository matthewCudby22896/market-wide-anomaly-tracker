CREATE TABLE bars_1sec (
    symbol TEXT,
    t      BIGINT NOT NULL, 
    o      DOUBLE PRECISION,
    h      DOUBLE PRECISION,
    l      DOUBLE PRECISION,
    c      DOUBLE PRECISION,
    n      BIGINT,
    v      DOUBLE PRECISION,
    vw     DOUBLE PRECISION,
    PRIMARY KEY (symbol, t)
);

SELECT create_hypertable('bars_1sec', by_range('t', 86400000));

CREATE TABLE hydration_state_1sec (
    symbol  TEXT,
    date    DATE,
    PRIMARY KEY (symbol, date)
);

-- CREATE OR REPLACE FUNCTION current_epoch_milliseconds() RETURNS BIGINT
-- LANGUAGE SQL STABLE AS $$ 
--     SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT; 
-- $$;

-- SELECT set_integer_now_func('bars_1sec', 'current_epoch_milliseconds', true);

-- CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1min
-- WITH (timescaledb.continuous) AS
-- SELECT 
--     symbol,
--     time_bucket(60000::BIGINT, t) AS bucket_1m, 
--     first(o, t) as o,
--     MAX(h) as h,
--     MIN(l) as l,
--     last(c, t) as c,
--     SUM(n) as n,
--     SUM(v) as v,
--     SUM(vw *v) / NULLIF(sum(v), 0) as vw
-- FROM bars_1sec
-- GROUP BY bucket_1m, symbol WITH NO DATA;

-- ALTER MATERIALIZED VIEW bars_1min set (timescaledb.materialized_only = false);