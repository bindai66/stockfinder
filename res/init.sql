CREATE TABLE IF NOT EXISTS CALENDAR (
    DATE            CHAR(8),
    WORK            CHAR(1),
    PREV_DATE       CHAR(8),
    NEXT_DATE       CHAR(8),
    PREV_BIZDATE    CHAR(8),
    NEXT_BIZDATE    CHAR(8),
    DATE_NO         INTEGER,
    BIZDATE_NO      INTEGER,
    PRIMARY KEY (DATE)
);

CREATE TABLE IF NOT EXISTS BASE_ITEM (
    SYMBOL          CHAR(6),
    HNAME           VARCHAR(40),
    MARKET          CHAR(3),
    BIZCODE         CHAR(6),
    BIZNAME         VARCHAR(40),
    LST_QTY         INTEGER,
    CAPITAL         INTEGER,
    PRIMARY KEY (SYMBOL)
);

CREATE TABLE IF NOT EXISTS POSITION (
    SYMBOL          CHAR(6),
    PUR_QTY         INT,
    PUR_AMT         INT,
    BUY_DATE        CHAR(8),
    PRIMARY KEY (SYMBOL)
);

CREATE TABLE IF NOT EXISTS OHLCV (
    SYMBOL          CHAR(6),
    DATE            CHAR(8),
    OPEN            INTEGER,
    HIGH            INTEGER,
    LOW             INTEGER,
    CLOSE           INTEGER,
    VOLUME          INTEGER,
    FOREIGN_RT      REAL,
    PRIMARY KEY (SYMBOL, DATE)
);

CREATE TABLE IF NOT EXISTS IND_MA (
    SYMBOL          CHAR(6),
    DATE            CHAR(8),
    P5              INTEGER,
    P10             INTEGER,
    P20             INTEGER,
    P60             INTEGER,
    V5              INTEGER,
    V10             INTEGER,
    V20             INTEGER,
    V60             INTEGER,
    PRIMARY KEY (SYMBOL, DATE)
);

CREATE TABLE IF NOT EXISTS IND_EMA (
    SYMBOL          CHAR(6),
    DATE            CHAR(8),
    P5              INTEGER,
    P10             INTEGER,
    P20             INTEGER,
    P60             INTEGER,
    V5              INTEGER,
    V10             INTEGER,
    V20             INTEGER,
    V60             INTEGER,
    PRIMARY KEY (SYMBOL, DATE)
);

CREATE TABLE IF NOT EXISTS IND_MACD (
    SYMBOL          CHAR(6),
    DATE            CHAR(8),
    P12             REAL,
    P26             REAL,
    MACD            REAL,
    SIGNAL          REAL,
    HIST            REAL,
    PRIMARY KEY (SYMBOL, DATE)
);

CREATE TABLE IF NOT EXISTS IND_RSI (
    SYMBOL          CHAR(6),
    DATE            CHAR(8),
    RSI             REAL,
    PRIMARY KEY (SYMBOL, DATE)
);

CREATE TABLE IF NOT EXISTS IND_STOCHASTIC (
    SYMBOL          CHAR(6),
    DATE            CHAR(8),
    K				REAL,
    D				REAL,
    PRIMARY KEY (SYMBOL, DATE)
);

CREATE TABLE IF NOT EXISTS IND_WIL_R (
    SYMBOL          CHAR(6),
    DATE            CHAR(8),
    WIL_R			REAL,
    PRIMARY KEY (SYMBOL, DATE)
);

