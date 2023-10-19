CREATE TABLE es_checkpoints (
  seq                  INTEGER         PRIMARY KEY AUTOINCREMENT,
  id                   UUID            NOT NULL,
  created              BIGINT          NOT NULL,
  updated              BIGINT          NOT NULL,
  sequence_id          TEXT
);
CREATE UNIQUE INDEX es_checkpoints_id ON es_checkpoints(id);
