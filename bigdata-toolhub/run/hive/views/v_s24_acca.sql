use zhgl;

CREATE VIEW if not exists V_S24_ACCA AS
SELECT XACCOUNT,
          BANK,
          SMS_FEES / 100 AS SMS_FEES,
          FEESIGN,
          SMS_LOWAMT,
          SMS_LOWAMX,
          SMS_YN,
          SMS_MONTH,
          MP_L_LMT,
          MP_AUTHS / 100 AS MP_AUTHS,
          MPAUSIGN,
          MP_REM_PPL / 100 AS MP_REM_PPL,
          MPREMSIG,
          MP_BAL / 100 AS MP_BAL,
          MPBALSIG,
          CAL_LIMIT,
          CAL_AUTHS / 100 AS CAL_AUTHS,
          CAL_AUTHS_FLAG,
          CAL_BAL / 100 AS CAL_BAL,
          CAL_BAL_FLAG,
          CAL_REMPPL / 100 AS CAL_REMPPL,
          CAL_REMPPL_FLAG,
          SMS_FREEYN,
          BAL_MPPL / 100 AS BAL_MPPL,
          BAL_MPPLX / 100 AS BAL_MPPLX,
          BAL_L_MPPL / 100 AS BAL_L_MPPL,
          DAILY_REP
     FROM S24_ACCA;

