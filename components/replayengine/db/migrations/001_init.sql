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

SELECT create_hypertable('bars_1sec', by_range('t', 86400000000));

CREATE TABLE hydration_state_1sec (
    symbol  TEXT,
    date    DATE,
    PRIMARY KEY (symbol, date)
)