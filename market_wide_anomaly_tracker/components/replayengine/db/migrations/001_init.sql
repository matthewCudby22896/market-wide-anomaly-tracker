CREATE TABLE bars_1sec (
    symbol TEXT,
    t      BIGINT,
    o      REAL,
    h      REAL,
    l      REAL,
    c      REAL,
    n      BIGINT,
    v      REAL,
    vw     REAL,
    PRIMARY KEY (symbol, t)
);

CREATE TABLE hydration_state_1sec (
    symbol  TEXT,
    date    DATE,
    PRIMARY KEY (symbol, date)
)