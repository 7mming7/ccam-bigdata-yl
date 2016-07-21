use zhgl;

CREATE VIEW if not exists V_FDM_S24_CUNEG AS
SELECT END_DATE,
          START_DATE,
          BANK,
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
     FROM FDM_S24_CUNEG
   UNION ALL
   SELECT SYSDATE(),
          p.START_DATE,
          p.BANK,
          p.CUSTR_NBR,
          p.FILE_TYPE,
          p.INP_DAY,
          p.INP_TIME,
          p.NAME_KEY,
          p.APPLNREF,
          p.CUSTR_REF,
          p.EMPLOYEE,
          p.NAME_EXTND,
          p.REASN_CODE,
          p.REASN_DESC,
          p.STATUS,
          p.CHG_DAY,
          p.CHG_TIME,
          p.CHG_EMPLOY,
          p.INP_SOURCE
     FROM FDM_S24_CUR_CUNEG p;


