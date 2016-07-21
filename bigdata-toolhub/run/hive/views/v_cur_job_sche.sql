use zhgl;

CREATE VIEW if not exists V_CUR_JOB_SCHE AS
SELECT a.JOB_SEQ_ID,
          a.JOB_NM,
          a.JOB_STATUS,
          a.JOB_PRIO,
          a.JOB_SCHE_DATE,
          a.JOB_BEGIN_DT,
          a.JOB_BEGIN_TM,
          a.JOB_END_DT,
          a.JOB_END_TM
     FROM job_sche a, SYSTEMPARA b
    WHERE a.JOB_SCHE_DATE = b.CURDATE;
