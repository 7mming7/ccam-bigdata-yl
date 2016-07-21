use zhgl;

CREATE VIEW if not exists V_S24_PCPDR AS
SELECT BANK,
          CURR_NUM,
          CP_NO,
          PRODUCT,
          MAX_AMT / 100 AS MAX_AMT,
          MIN_AMT / 100 AS MIN_AMT,
          SIGN_MIN_AMT,
          NBR_MTHS0,
          PROD_ID0,
          NBR_MTHS1,
          PROD_ID1,
          NBR_MTHS2,
          PROD_ID2,
          NBR_MTHS3,
          PROD_ID3,
          NBR_MTHS4,
          PROD_ID4,
          NBR_MTHS5,
          PROD_ID5,
          NBR_MTHS6,
          PROD_ID6,
          NBR_MTHS7,
          PROD_ID7,
          NBR_MTHS8,
          PROD_ID8,
          NBR_MTHS9,
          PROD_ID9
     FROM S24_PCPDR;


