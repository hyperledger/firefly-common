CREATE TABLE crudables (
  seq         INTEGER         PRIMARY KEY AUTOINCREMENT,
  id          UUID            NOT NULL,
  created     BIGINT          NOT NULL,
  updated     BIGINT          NOT NULL,
  ns          VARCHAR(64)     NOT NULL,
  name        TEXT,
  field1      TEXT,
  field2      VARCHAR(65),
  field3      TEXT
);
CREATE UNIQUE INDEX crudables_id ON crudables(ns, id);
