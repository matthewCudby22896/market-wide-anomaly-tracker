CREATE TABLE ohlc_bars (
    ticker TEXT,
    t      BIGINT,
    o      REAL,
    h      REAL,
    l      REAL,
    c      REAL,
    v      REAL,
    vw     REAL,
    PRIMARY KEY (ticker, t)
);