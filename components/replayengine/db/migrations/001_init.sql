CREATE TABLE bars_1sec (
    symbol TEXT,
    t      BIGINT,
    o      DOUBLE PRECISION,
    h      DOUBLE PRECISION,
    l      DOUBLE PRECISION,
    c      DOUBLE PRECISION,
    n      BIGINT,
    v      DOUBLE PRECISION,
    vw     DOUBLE PRECISION,
    PRIMARY KEY (symbol, t)
);

CREATE TABLE hydration_state_1sec (
    symbol  TEXT,
    date    DATE,
    PRIMARY KEY (symbol, date)
)