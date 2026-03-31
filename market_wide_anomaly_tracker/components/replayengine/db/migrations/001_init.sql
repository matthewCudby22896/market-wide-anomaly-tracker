CREATE TABLE ohlc_bars (
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