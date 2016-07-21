use zhgl;

CREATE VIEW if not exists V_S24_STMT_X_6M AS
SELECT T1.BANK,
          T1.XACCOUNT,
          T1.MONTH_NBR,
          T1.PAYMENT,
          T1.PURCHASES,
          T1.CLOSE_BAL,
          T1.BAL                                                  --余额（不含大额分期）
                ,
          T1.BAL_ADD14                                             --余额（含大额分期）
                      ,
          T1.OPEN_BAL,
          T1.CRED_LIMIT,
          T1.C_LIMIT                                       --当前额度（有临时额度的算临时额度）
                    ,
          T1.LIMIT                                                 --当前额度含现金分期
                  ,
          T1.LIMIT_MP                                         --当前额度含现金分期和大额分期
                     ,
          T1.POINT_EAR,
          T1.NBR_CASHAD_NEW                                           --预借现金笔数
                           ,
          T1.ODUE_MTHS                                                  --逾期期数
                      ,
          T1.NBR_PURCH_NEW                                            --一般消费笔数
                          ,
          CASE
             WHEN     (COAL (T2.SHOPPING_POINT, 0) + 0) >=
                         COAL (T1.CRED_LIMIT, 0)
                  AND COAL (T2.SHOPPING_POINT, 0) > 0
             THEN
                100
             WHEN COAL (T1.CRED_LIMIT, 0) <= 0
             THEN
                0
             WHEN COAL (T2.SHOPPING_POINT, 0) <= 0
             THEN
                0
             ELSE
                ROUND (
                     COAL (T2.SHOPPING_POINT, 0)
                   / COAL (T1.CRED_LIMIT, 0)
                   * 100,
                   2)
          END
             AS RATIO_CREDIT_USE_SP                                --额度使用率(积分)
                                   ,
          CASE
             WHEN    COAL (T1.OPEN_BAL, 0) <= 0
                  OR (COAL (T1.PAYMENT, 0) + 0) > COAL (T1.OPEN_BAL, 0)
             THEN
                100
             ELSE
                ROUND (COAL (T1.PAYMENT, 0) / T1.OPEN_BAL * 100, 2)
          END
             AS PAYMENT_RATIO                                            --还款率
                             ,
          CASE
             WHEN    COAL (T1.OPEN_BAL, 0) <= 0
                  OR (COAL (T1.PAYMENT, 0) + 0) > COAL (T1.OPEN_BAL, 0)
             THEN
                1
             WHEN COAL (T1.PAYMENT, 0) * 10 > T1.OPEN_BAL
             THEN
                1
             ELSE
                0
          END
             AS FLAG_PAYMENT_P10                                  --还款率10%以上标志
                                ,
          COAL (T1.BAL_NINT, 0) AS TOT_COST --   + COAL(T1.PEN_CHRG,0) + COAL(T1.OTHER_FEES,0)
                                               --费用合计
          ,
          (COAL (T1.INT_CURCMP, 0) + 0) + COAL (T1.INT_EARNED, 0)
             AS TOT_INI                                                 --利息合计
                       ,
          CASE
             WHEN   COAL (T1.BAL_NINT, 0)
                  + COAL (T1.NBR_OTHERS_NEW, 0)
                  + COAL (T1.NBR_CASHAD_NEW, 0)
                  + COAL (T1.PEN_CHRG, 0)
                  + COAL (T1.OTHER_FEES, 0) > 0
             THEN
                1
             ELSE
                0
          END
             AS FLAG_COST                                               --费用标志
                         ,
          COAL (T1.AGE, 0) AS TOT_AGE                             --逾期金额合计
                                         ,
          CASE WHEN COAL (T1.AGE, 0) > 0 THEN 1 ELSE 0 END AS FLAG_AGE --逾期标志
                                                                          ,
          CASE
             WHEN (COAL (T1.AGE, 0) + 0) > COAL (T3.AGE, 0) THEN 1
             ELSE 0
          END
             AS FLAG_CON_ADD_AGE                                   ,
          CASE
             WHEN (COAL (T1.ODUE_FLAG, 0) + 0) + COAL (T3.ODUE_FLAG, 0) >=  1
             THEN
                1
             ELSE
                0
          END
             AS FLAG_CON_AGE                                          --连续逾期标志
                            ,
          CASE
             WHEN (COAL (T1.CLOSE_BAL, 0) + 0) > COAL (T1.OPEN_BAL, 0)
             THEN
                1
             ELSE
                0
          END
             AS FLAG_CON_ADD_BAL                                    --余额连续增加标志
                                ,
          CASE WHEN COAL (T1.NBR_CASHAD_NEW, 0) > 0 THEN 1 ELSE 0 END
             AS FLAG_CASH                                               --取现标志
                         ,
          CASE
             WHEN     COAL (T3.ODUE_FLAG, 0) > 0
                  AND COAL (T3.ODUE_HELD, 0) > 300
                  AND COAL (T1.ODUE_FLAG, 0) > 0
                  AND COAL (T1.ODUE_HELD, 0) > 300
             THEN
                1
             ELSE
                0
          END
             AS FLAG_CON_ODUE                                   --连续逾期贷款超300标志
                             ,
          CASE
             WHEN     COAL (T1.CASH_ADVCE, 0) > 0
                  AND COAL (T1.CLOSE_BAL, 0) > 0
                  AND COAL (T3.CASH_ADVCE, 0) > 0
                  AND COAL (T3.CLOSE_BAL, 0) > 0
                  AND   COAL (T1.CASH_ADVCE, 0)
                      / COAL (T1.CLOSE_BAL, 0) >
                           COAL (T3.CASH_ADVCE, 0)
                         / COAL (T3.CLOSE_BAL, 0)
             THEN
                1
             ELSE
                0
          END
             AS FLAG_CON_ADD_CASH                   --取现金额/当期发生额（不含还款调账）连续增加标志
                                 ,
          CASE
             WHEN (COAL (T1.BAL_NINT, 0) + 0) > COAL (T3.BAL_NINT, 0)
             THEN
                1
             ELSE
                0
          END
             AS FLAG_CON_ADD_COST,
          T1.STMT_DATE
     FROM MDM_STMT_X T1
          LEFT JOIN
          (  SELECT P.ACCOUNT AS XACCOUNT,
                    P.MONTH_NBR,
                    SUM (
                       CASE
                          WHEN (P.PTLG_TYPE + 0) >= '10' AND P.PTLG_TYPE <= '19'
                          THEN
                             POINT
                          ELSE
                             0
                       END)
                       AS SHOPPING_POINT
               FROM FDM_S24_PTLOG P
              WHERE     P.CREATE_DATE <= DATE_ADD(SYSDATE() ,1)
                    AND P.CREATE_DATE > ADD_MONTHS (SYSDATE(), -6)
           GROUP BY P.ACCOUNT, P.MONTH_NBR) T2
             ON T1.XACCOUNT = T2.XACCOUNT AND T1.MONTH_NBR = T2.MONTH_NBR
          LEFT JOIN MDM_STMT_X T3
             ON     T1.XACCOUNT = T3.XACCOUNT
                AND T1.MONTH_NBR - 1 = T3.MONTH_NBR
                AND t3.STMT_DATE <= ADD_MONTHS (SYSDATE(), -1)
                AND t3.STMT_DATE > ADD_MONTHS (SYSDATE(), -7)
    WHERE     todate(t1.STMT_DATE,'yyyy-mm-dd') <= DATE_ADD(SYSDATE() ,1)
          AND todate(t1.STMT_DATE,'yyyy-mm-dd') > ADD_MONTHS (SYSDATE(), -6)
;

