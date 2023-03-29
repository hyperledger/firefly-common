CREATE TABLE linkables (
  seq         INTEGER         PRIMARY KEY AUTOINCREMENT,
  id          UUID            NOT NULL,
  ns          VARCHAR(64)     NOT NULL,
  desc        TEXT,
  crud_id     UUID
);
CREATE UNIQUE INDEX linkables_id ON linkables(ns, id)