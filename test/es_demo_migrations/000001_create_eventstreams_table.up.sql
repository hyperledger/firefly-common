CREATE TABLE eventstreams (
  seq                  INTEGER         PRIMARY KEY AUTOINCREMENT,
  id                   UUID            NOT NULL,
  created              BIGINT          NOT NULL,
  updated              BIGINT          NOT NULL,
  name                 VARCHAR(64)     NOT NULL,
  status               VARCHAR(64),
  type                 VARCHAR(64),
  initial_sequence_id  TEXT,
  topic_filter         TEXT,
  config               TEXT,
  error_handling       TEXT,
  batch_size           INT,
  batch_timeout        BIGINT,
  retry_timeout        BIGINT,
  blocked_retry_delay  BIGINT,
  webhook_config       TEXT,
  websocket_config     TEXT
);
CREATE UNIQUE INDEX eventstreams_id ON eventstreams(id);
CREATE UNIQUE INDEX eventstreams_name ON eventstreams(name);
