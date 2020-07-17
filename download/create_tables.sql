DROP TABLE IF EXISTS request;
DROP TABLE IF EXISTS package;

CREATE TABLE request (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset_id VARCHAR UNIQUE,
  status VARCHAR DEFAULT 'pending',
  initiated DATETIME DEFAULT (datetime('now'))
);

CREATE TABLE package (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset_id VARCHAR NOT NULL,
  filename VARCHAR UNIQUE NOT NULL,
  size_bytes INTEGER,
  checksum VARCHAR,
  generation_started DATETIME DEFAULT (datetime('now')),
  generation_completed DATETIME
);

