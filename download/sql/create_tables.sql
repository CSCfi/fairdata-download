CREATE TABLE IF NOT EXISTS package (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename VARCHAR UNIQUE,
  size_bytes INTEGER,
  checksum VARCHAR,
  generated_by VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS download (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token VARCHAR UNIQUE NOT NULL,
  filename VARCHAR NOT NULL,
  status VARCHAR DEFAULT ('ACTIVE'),
  started DATETIME DEFAULT (datetime('now')),
  finished DATETIME
);

CREATE TABLE IF NOT EXISTS generate_task (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  task_id VARCHAR(155),
  dataset_id VARCHAR(155),
  is_partial NUMBER(1) NOT NULL,
  status VARCHAR(50),
  initiated DATETIME DEFAULT (datetime('now')),
  date_done DATETIME,
  result BLOB,
  traceback TEXT,
  retries INTEGER,
  UNIQUE (task_id)
);

CREATE TABLE IF NOT EXISTS generate_scope (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  task_id VARCHAR(155) NOT NULL,
  filepath VARCHAR(512) NOT NULL
);

CREATE TABLE IF NOT EXISTS generate_taskgroup (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  taskset_id VARCHAR(155),
  result BLOB,
  date_done DATETIME,
  UNIQUE (taskset_id)
);

CREATE TABLE IF NOT EXISTS generate_request (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  task_id VARCHAR(155) NOT NULL
);

CREATE TABLE IF NOT EXISTS generate_request_scope (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL,
  prefix VARCHAR(512) NOT NULL
);

CREATE TABLE IF NOT EXISTS subscription (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id VARCHAR(155),
  notify_url VARCHAR,
  subscription_data BLOB
);
