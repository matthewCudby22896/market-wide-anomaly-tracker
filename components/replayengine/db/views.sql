-- Run this first
CREATE OR REPLACE FUNCTION current_epoch_microseconds() RETURNS BIGINT
LANGUAGE SQL STABLE AS $$ 
    SELECT (EXTRACT(EPOCH FROM NOW()) * 1000000)::BIGINT; 
$$;

-- Register the function to your hypertable
SELECT set_integer_now_func('bars_1sec', 'current_epoch_microseconds');

CREATE MATERIALIZED VIEW bars_1min
WITH (timescaledb.continuous) AS
SELECT 
    symbol,
    time_bucket(60000::BIGINT, t) AS bucket_1m, 
    first(o, t) as o,
    MAX(h) as h,
    MIN(l) as l,
    last(c, t) as c,
    SUM(n) as n,
    SUM(v) as v,
    SUM(vw *v) / NULLIF(sum(v), 0) as vw
FROM bars_1sec
GROUP BY bucket_1m, symbol WITH NO DATA;