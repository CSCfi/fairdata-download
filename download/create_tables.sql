DROP TABLE IF EXISTS generate_task;
DROP TABLE IF EXISTS generate_taskgroup;
DROP TABLE IF EXISTS package;
DROP TABLE IF EXISTS download;

CREATE TABLE package (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset_id VARCHAR NOT NULL,
  task_id VARCHAR NOT NULL,
  filename VARCHAR UNIQUE,
  size_bytes INTEGER,
  checksum VARCHAR,
  initiated DATETIME DEFAULT (datetime('now'))
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
  status VARCHAR(50), 
  result BLOB, 
  date_done DATETIME, 
  traceback TEXT, 
  name VARCHAR(155), 
  args BLOB, 
  kwargs BLOB, 
  worker VARCHAR(155), 
  retries INTEGER, 
  queue VARCHAR(155), 
  UNIQUE (task_id)
);

CREATE TABLE generate_taskgroup (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
  taskset_id VARCHAR(155), 
  result BLOB, 
  date_done DATETIME, 
  UNIQUE (taskset_id)
);
