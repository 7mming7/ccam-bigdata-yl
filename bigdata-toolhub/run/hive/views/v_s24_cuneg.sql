use zhgl;

CREATE VIEW if not exists V_S24_CUNEG AS
SELECT BANK,
          CUSTR_NBR,
          FILE_TYPE,
          INP_DAY,
          INP_TIME,
          NAME_KEY,
          APPLNREF,
          CUSTR_REF,
          EMPLOYEE,
          NAME_EXTND,
          REASN_CODE,
          REASN_DESC,
          STATUS,
          CHG_DAY,
          CHG_TIME,
          CHG_EMPLOY,
          INP_SOURCE
     FROM S24_CUNEG;

