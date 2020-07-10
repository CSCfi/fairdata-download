DROP TABLE IF EXISTS request;

CREATE TABLE request (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset_id VARCHAR UNIQUE,
  status VARCHAR DEFAULT 'pending',
  initiated DATETIME DEFAULT (datetime('now'))
);

