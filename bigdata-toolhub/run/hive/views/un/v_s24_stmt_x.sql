use zhgl;
CREATE VIEW if not exists V_S24_STMT_X AS
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
             WHEN     COALESCE (T2.SHOPPING_POINT, 0) >= COALESCE (T1.CRED_LIMIT, 0)
                  AND COALESCE (T2.SHOPPING_POINT, 0) > 0
             THEN
                100
             WHEN COALESCE (T1.CRED_LIMIT, 0) <= 0
             THEN
                0
             WHEN COALESCE (T2.SHOPPING_POINT, 0) <= 0
             THEN
                0
             ELSE
                ROUND (
                     COALESCE (T2.SHOPPING_POINT, 0)
                   / COALESCE (T1.CRED_LIMIT, 0)
                   * 100,
                   2)
          END
             AS RATIO_CREDIT_USE_SP                               
                                   ,
          CASE
             WHEN    COALESCE (T1.OPEN_BAL, 0) <= 0
                  OR COALESCE (T1.PAYMENT, 0) > COALESCE (T1.OPEN_BAL, 0)
             THEN
                100
             ELSE
                ROUND (COALESCE (T1.PAYMENT, 0) / T1.OPEN_BAL * 100, 2)
          END
             AS PAYMENT_RATIO                                           
                             ,
          CASE
             WHEN    COALESCE (T1.OPEN_BAL, 0) <= 0
                  OR COALESCE (T1.PAYMENT, 0) > COALESCE (T1.OPEN_BAL, 0)
             THEN
                1
             WHEN COALESCE (T1.PAYMENT, 0) * 10 > T1.OPEN_BAL
             THEN
                1
             ELSE
                0
          END
             AS FLAG_PAYMENT_P10                                 
                                ,
          COALESCE (T1.BAL_NINT, 0) AS TOT_COST 
                                            
          ,
          COALESCE (T1.INT_CURCMP, 0) + COALESCE (T1.INT_EARNED, 0)
             AS TOT_INI                                                 
                       ,
          CASE
             WHEN   COALESCE (T1.BAL_NINT, 0)
                  + COALESCE (T1.NBR_OTHERS_NEW, 0)
                  + COALESCE (T1.NBR_CASHAD_NEW, 0)
                  + COALESCE (T1.PEN_CHRG, 0)
                  + COALESCE (T1.OTHER_FEES, 0) > 0
             THEN
                1
             ELSE
                0
          END
             AS FLAG_COST                                             
                         ,
          COALESCE (T1.AGE, 0) AS TOT_AGE                           
                                         ,
          CASE WHEN COALESCE (T1.AGE, 0) > 0 THEN 1 ELSE 0 END AS FLAG_AGE 
                                                                          ,
          CASE
             WHEN COALESCE (T1.AGE, 0) > COALESCE (T3.AGE, 0) THEN 1
             ELSE 0
          END
             AS FLAG_CON_ADD_AGE,                               
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
           GROUP BY P.ACCOUNT, P.MONTH_NBR) T2
             ON T1.XACCOUNT = T2.XACCOUNT AND T1.MONTH_NBR = T2.MONTH_NBR
          LEFT JOIN V_FDM_S24_ACCA T4
             ON     T1.BANK = T4.BANK
                AND T1.XACCOUNT = T4.XACCOUNT
                AND T1.STMT_DATE + 1 >= T4.END_DATE
                AND T1.STMT_DATE < T4.END_DATE
          LEFT JOIN MDM_ACCT_X T5 ON T1.XACCOUNT = T5.XACCOUNT
          LEFT JOIN MDM_STMT_X T3
             ON T1.XACCOUNT = T3.XACCOUNT AND T1.MONTH_NBR - 1 = T3.MONTH_NBR
;

