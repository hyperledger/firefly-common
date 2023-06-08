CREATE TABLE history (
  seq         INTEGER         PRIMARY KEY AUTOINCREMENT,
  id          UUID            NOT NULL,
  created     BIGINT          NOT NULL,
  subject     TEXT,
  info        TEXT
);
CREATE UNIQUE INDEX history_id ON history(id);
CREATE INDEX history_subject ON history(subject);
