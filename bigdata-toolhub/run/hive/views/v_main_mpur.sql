use zhgl;

CREATE VIEW if not exists V_MAIN_MPUR AS
SELECT t0.custnum,
          t0.CURR_TERM,
          t0.CU_LINS_CUR_FLG,
          t0.LINS_DATE_END,
          t0.LINS_DATE_ST,
          t0.LINS_TYPE,
          t0.LINS_TERM,
          t0.LINS_REM_TERM,
          t0.LINS_UNSH_PRINCIPAL,
          t0.LINS_STATUS
     FROM account t0,
          (  SELECT t1.CUSTNUM, MAX (t1.LINS_DATE_ST + 0) AS LINS_DATE_ST
               FROM account t1
           GROUP BY t1.CUSTNUM) maxmpur
    WHERE     t0.custnum = maxmpur.custnum
          AND t0.LINS_DATE_ST = maxmpur.LINS_DATE_ST
          AND maxmpur.LINS_DATE_ST <> 0;


