use zhgl;

CREATE VIEW if not exists V_S24_NOTES AS
SELECT BANK,
          XACCOUNT,
          ENTRY_DAY,
          ENTRY_SEQ,
          ENTRY_TIME,
          ENTRY_TYPE,
          AMT_INVLVD / 100 AS AMT_INVLVD,
          BUSINESS,
          CARD_NBR,
          CUSTR_NBR,
          ENTRY_BRCH,
          ENTRY_SRCE,
          LETR_CODE,
          NARR_1,
          OPERATOR,
          QUERY_CODE,
          RESLN_DAY,
          RESLN_MTHD,
          RESLN_TIME,
          SHORT_DESC,
          NARR_2,
          NARR_3,
          NARR_4
     FROM S24_NOTES;

