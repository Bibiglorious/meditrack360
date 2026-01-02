CREATE TABLE IF NOT EXISTS meditrack.fact_lab_turnaround (
  fact_key           BIGINT IDENTITY(1,1),

  lab_result_id      VARCHAR(64)  NOT NULL,
  patient_id         VARCHAR(64)  NOT NULL,
  admission_id       VARCHAR(64),

  test_type          VARCHAR(255) NOT NULL,

  collected_time     TIMESTAMP,
  completed_time     TIMESTAMP,
  result_date        DATE,

  turnaround_minutes INTEGER,


  PRIMARY KEY (fact_key)
)
DISTSTYLE AUTO
SORTKEY (result_date, test_type);