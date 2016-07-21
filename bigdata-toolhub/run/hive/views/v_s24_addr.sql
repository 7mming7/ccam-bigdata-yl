use zhgl;

CREATE VIEW if not exists V_S24_ADDR AS
SELECT BANK,
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
     FROM S24_ADDR;

