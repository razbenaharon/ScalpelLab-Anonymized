CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE analysis_information (
  date_case TEXT PRIMARY KEY,
  labeled   INTEGER,  -- 0/1 boolean
  label_by  TEXT,
  FOREIGN KEY (date_case) REFERENCES "date_case_directory_raz"(date_case) ON DELETE CASCADE
);
CREATE TABLE video_path (
  date_case   TEXT NOT NULL,
  camera_name TEXT NOT NULL,
  seq_path    TEXT,          -- path to SEQ
  mp4_path    TEXT,          -- path to MP4
  recorded_ts TEXT,          -- 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
  size_bytes  INTEGER,
  status      TEXT,
  PRIMARY KEY (date_case, camera_name),
  FOREIGN KEY (date_case) REFERENCES "date_case_directory_raz"(date_case) ON DELETE CASCADE,
  CHECK (size_bytes IS NULL OR size_bytes >= 0),
  CHECK (status IS NULL OR status IN ('ok','missing','broken','converted','queued'))
);
CREATE TABLE IF NOT EXISTS "resident" (
    intern_key     INTEGER PRIMARY KEY,
    name           TEXT NOT NULL,
    code           TEXT,
    res_start_date TEXT,
    grade_a_date   TEXT   -- merged column, no CHECK
);
CREATE TABLE IF NOT EXISTS "record_status"
(
    date_case          TEXT
        primary key
        constraint record_status_pk
            unique
        constraint record_status_intern_date_full_date_case_fk
            references intern_date_full,
    Cart_Center_2      INTEGER default 0,
    Cart_LT_4          INTEGER default 0,
    Cart_RT_1          INTEGER default 0,
    General_3          INTEGER default 0,
    Monitor            INTEGER default 0,
    Patient_Monitor    INTEGER default 0,
    Ventilator_Monitor INTEGER default 0,
    Injection_Port     INTEGER default 0,
    comments           TEXT,
    check (Cart_Center_2 >= 0 AND
           Cart_LT_4 >= 0 AND
           Cart_RT_1 >= 0 AND
           General_3 >= 0 AND
           Monitor >= 0 AND
           Patient_Monitor >= 0 AND
           Ventilator_Monitor >= 0 AND
           Injection_Port >= 0)
);
CREATE TABLE IF NOT EXISTS "intern_date_full"
(
    recording_date TEXT,
    signature_time TEXT,
    case_no        INT,
    date_case      TEXT
        constraint intern_date_full_pk
            primary key
        constraint intern_date_full_pk_2
            unique,
    code           TEXT,
    intern_key     INT
        constraint intern_date_full_resident_intern_key_fk
            references resident
);
