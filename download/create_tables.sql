DROP TABLE IF EXISTS generate_task;
DROP TABLE IF EXISTS generate_scope;
DROP TABLE IF EXISTS generate_taskgroup;
DROP TABLE IF EXISTS package;
DROP TABLE IF EXISTS download;

CREATE TABLE package (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename VARCHAR UNIQUE,
  size_bytes INTEGER,
  checksum VARCHAR,
  generated_by VARCHAR NOT NULL
);

CREATE TABLE download (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token VARCHAR UNIQUE NOT NULL,
  filename VARCHAR NOT NULL,
  download_started DATETIME DEFAULT (datetime('now'))
);

CREATE TABLE generate_task (
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

CREATE TABLE generate_scope (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
  task_id VARCHAR(155) NOT NULL, 
  filepath VARCHAR(512) NOT NULL
);

CREATE TABLE generate_taskgroup (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
  taskset_id VARCHAR(155), 
  result BLOB, 
  date_done DATETIME, 
  UNIQUE (taskset_id)
);
