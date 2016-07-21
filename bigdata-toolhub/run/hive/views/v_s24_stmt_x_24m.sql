use zhgl;

CREATE VIEW if not exists V_S24_STMT_X_24M AS
SELECT T1.BANK,
          T1.XACCOUNT,
          T1.MONTH_NBR,
          T1.PAYMENT,
          T1.PURCHASES,
          T1.CLOSE_BAL,
          T1.BAL                                           
                ,
          T1.BAL_ADD14                                     
                      ,
          T1.OPEN_BAL,
          T1.CRED_LIMIT,
          T1.C_LIMIT                                       
                    ,
          T1.LIMIT                                         
                  ,
          T1.LIMIT_MP                                      
                     ,
          T1.POINT_EAR,
          T1.NBR_CASHAD_NEW                                
                           ,
          T1.ODUE_MTHS                                     
                      ,
          T1.NBR_PURCH_NEW                                 
                          ,
          CASE
             WHEN     COAL (T2.SHOPPING_POINT, 0) >=
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
             AS RATIO_CREDIT_USE_SP                                
                                   ,
          CASE
             WHEN    COAL (T1.OPEN_BAL, 0) <= 0
                  OR (COAL (T1.PAYMENT, 0) + 0) > COAL (T1.OPEN_BAL, 0)
             THEN
                100
             ELSE
                ROUND (COAL (T1.PAYMENT, 0) / T1.OPEN_BAL * 100, 2)
          END
             AS PAYMENT_RATIO                                         
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
             AS FLAG_PAYMENT_P10                
                                ,
          COAL (T1.BAL_NINT, 0) AS TOT_COST 
                                               
          ,
          (COAL (T1.INT_CURCMP, 0) + 0) + COAL (T1.INT_EARNED, 0)
             AS TOT_INI                                              
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
             AS FLAG_COST                                            
                         ,
          COAL (T1.AGE, 0) AS TOT_AGE                            
                                         ,
          CASE WHEN COAL (T1.AGE, 0) > 0 THEN 1 ELSE 0 END AS FLAG_AGE 
                                                                          ,
          CASE
             WHEN (COAL (T1.AGE, 0) + 0) > COAL (T3.AGE, 0) THEN 1
             ELSE 0
          END
             AS FLAG_CON_ADD_AGE,CASE WHEN (COAL (T1.ODUE_FLAG, 0) + 0) + COAL (T3.ODUE_FLAG, 0) >= 1
             THEN 1 ELSE 0 END AS FLAG_CON_AGE ,
          CASE WHEN (COAL (T1.CLOSE_BAL, 0) + 0) > COAL (T1.OPEN_BAL, 0)
             THEN 1 ELSE 0 END AS FLAG_CON_ADD_BAL ,
          CASE WHEN COAL (T1.NBR_CASHAD_NEW, 0) > 0 THEN 1 ELSE 0 END
             AS FLAG_CASH
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
             AS FLAG_CON_ODUE 
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
             AS FLAG_CON_ADD_CASH
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
                          WHEN P.PTLG_TYPE >= '10' AND P.PTLG_TYPE <= '19'
                          THEN
                             POINT
                          ELSE
                             0
                       END)
                       AS SHOPPING_POINT
               FROM FDM_S24_PTLOG P
              WHERE     P.CREATE_DATE <= DATE_ADD(SYSDATE() ,1)
                    AND P.CREATE_DATE > ADD_MONTHS (SYSDATE(), -24)
           GROUP BY P.ACCOUNT, P.MONTH_NBR) T2
             ON T1.XACCOUNT = T2.XACCOUNT AND T1.MONTH_NBR = T2.MONTH_NBR
          LEFT JOIN MDM_STMT_X T3
             ON     T1.XACCOUNT = T3.XACCOUNT
                AND T1.MONTH_NBR - 1 = T3.MONTH_NBR
                AND todate(t3.STMT_DATE,'yyyy-mm-dd') <= ADD_MONTHS (SYSDATE(), -1)
                AND todate(t3.STMT_DATE,'yyyy-mm-dd') > ADD_MONTHS (SYSDATE(), -25)
    WHERE     todate(t1.STMT_DATE,'yyyy-mm-dd') <= DATE_ADD(SYSDATE() ,1)
          AND todate(t1.STMT_DATE,'yyyy-mm-dd') > ADD_MONTHS (SYSDATE(), -24)
;

