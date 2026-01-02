CREATE TABLE IF NOT EXISTS meditrack.dim_date (
  date_key      INTEGER   NOT NULL,   
  full_date     DATE      NOT NULL,
  year          SMALLINT  NOT NULL,
  month         SMALLINT  NOT NULL,
  day           SMALLINT  NOT NULL,
  day_of_week   SMALLINT  NOT NULL,    
  week_of_year  SMALLINT,
  month_name    VARCHAR(12),
  day_name      VARCHAR(12),
  is_weekend    BOOLEAN,

  PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (full_date);




CREATE TABLE IF NOT EXISTS meditrack.dim_patient (
  patient_key   BIGINT IDENTITY(1,1),
  patient_id    VARCHAR(64) NOT NULL,

  PRIMARY KEY (patient_key)
)
DISTSTYLE AUTO
SORTKEY (patient_id);




CREATE TABLE IF NOT EXISTS meditrack.dim_test_type (
  test_type_key BIGINT IDENTITY(1,1),
  test_type     VARCHAR(255) NOT NULL,

  PRIMARY KEY (test_type_key)
)
DISTSTYLE AUTO
SORTKEY (test_type);