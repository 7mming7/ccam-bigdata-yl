CREATE DATABASE IF NOT EXISTS yinlian;

USE yinlian;

DROP TABLE IF EXISTS JOB_SCHE;

CREATE TABLE JOB_SCHE (
  JOB_SEQ_ID    int(11) NOT NULL AUTO_INCREMENT,
  JOB_NM        varchar(40),
  JOB_TYPE      varchar(10),
  JOB_STATUS    varchar(8),
  JOB_PRIO      int(11),
  JOB_SCHE_DATE date,
  JOB_BEGIN_DT  date,
  JOB_BEGIN_TM  datetime,
  JOB_END_DT    date,
  JOB_END_TM    datetime,
  SYS_ID        varchar(20),
  PRIMARY KEY ( JOB_SEQ_ID )
);
