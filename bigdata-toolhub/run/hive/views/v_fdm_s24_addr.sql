use zhgl;

CREATE VIEW if not exists V_FDM_S24_ADDR AS
SELECT END_DATE,
          START_DATE,
          BANK,
          CUSTR_NBR,
          ADDR_LINE1,
          ADDR_LINE2,
          ADDR_LINE3,
          ADDR_LINE4,
          ADDR_LINE5,
          POST_CODE,
          STATE_C,
          ADDR_TYPE,
          REC_TYPE,
          MERCHANT,
          OSEAADDR_F,
          CHANGE_DAY,
          CHANGE_TME,
          CREATE_DAY,
          CREATE_TME
     FROM FDM_S24_ADDR
   UNION ALL
   SELECT SYSDATE(),
          p.START_DATE,
          p.BANK,
          p.CUSTR_NBR,
          p.ADDR_LINE1,
          p.ADDR_LINE2,
          p.ADDR_LINE3,
          p.ADDR_LINE4,
          p.ADDR_LINE5,
          p.POST_CODE,
          p.STATE_C,
          p.ADDR_TYPE,
          p.REC_TYPE,
          p.MERCHANT,
          p.OSEAADDR_F,
          p.CHANGE_DAY,
          p.CHANGE_TME,
          p.CREATE_DAY,
          p.CREATE_TME
     FROM FDM_S24_CUR_ADDR p;
