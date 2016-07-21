use zhgltest;
CREATE VIEW if not exists V_CUSTNUM_SPECIAL_SP AS
SELECT ACCT.CUSTR_NBR AS CUSTR_NBR,
          CASE
             WHEN basic.LIMIT = 0
             THEN
                0
             ELSE
                CASE
                   WHEN ROUND (basic.BAL / basic.LIMIT, 4) * 100 > 100
                   THEN
                      100
                   ELSE
                      ROUND (basic.BAL / basic.LIMIT, 4) * 100
                END
          END
             LimitUsage,
          basic.BAL BAL,
          basic.BAL_ADD14 BAL_ADD14,
          basic.BAL_WO_AUTHS BAL_WO_AUTHS,
          m12.STMTRepayRatio_L12M STMTRepayRatio_L12M,
          m12.STMTRepayAveRatio_L12M STMTRepayAveRatio_L12M,
          m3.last3monavgrate last3monavgrate,
          m6.STMTUtility_Ave_L6M STMTUtility_Ave_L6M,
          m6.STMTRepayRatio_L6M STMTRepayRatio_L6M,
          CASE
             WHEN basic.LIMIT_MP = 0
             THEN
                0
             ELSE
                CASE
                   WHEN ROUND (basic.BAL_ADD14 / basic.LIMIT_MP, 4) * 100 >
                           100
                   THEN
                      100
                   ELSE
                      ROUND (basic.BAL_ADD14 / basic.LIMIT_MP, 4) * 100
                END
          END
             total_UsePre,
          bal_m6.month_count,
          a.maxproductline,
          m6.points_6m points_6m
     FROM FDM_S24_CUR_CUSTR acct
          LEFT JOIN
          (  SELECT A.CUSTR_NBR,
                    SUM (A.BAL) bal,
                    SUM (A.BAL_ADD14) BAL_ADD14,
                    SUM (A.BAL_WO_AUTHS) BAL_WO_AUTHS,
                    MAX (
                       CASE
                          WHEN     (A.TLMT_BEG + 0) <= todate (sysdate(), 'yyyymmdd')
                               AND todate (sysdate(), 'yyyymmdd') < (A.TLMT_END + 0)
                               AND A.TEMP_LIMIT > 0
                          THEN
                             (A.TEMP_LIMIT + 0)
                          ELSE
                             (A.CRED_LIMIT + 0)
                       END)
                       AS C_LIMIT,
                    MAX (
                       CASE
                          WHEN     (A.TLMT_BEG + 0) <= todate (sysdate(), 'yyyymmdd')
                               AND todate (sysdate(), 'yyyymmdd') < (A.TLMT_END + 0)
                               AND A.TEMP_LIMIT > 0
                          THEN
                             (A.TEMP_LIMIT + 0)
                          ELSE
                             (A.CRED_LIMIT + 0)
                       END)
                       AS LIMIT
                               ,
                      MAX (
                         CASE
                            WHEN     (A.TLMT_BEG + 0) <=
                                        todate (sysdate(), 'yyyymmdd')
                                 AND todate (sysdate(), 'yyyymmdd') < (A.TLMT_END + 0)
                                 AND A.TEMP_LIMIT > 0
                            THEN
                               (A.TEMP_LIMIT + 0)
                            ELSE
                               (A.CRED_LIMIT + 0)
                         END)
                    + SUM (A.MP_L_LMT)
                       AS LIMIT_MP
               FROM MDM_ACCT_X_SP A
           GROUP BY A.CUSTR_NBR) basic
             ON ACCT.CUSTR_NBR = basic.CUSTR_NBR
          LEFT JOIN
          (  SELECT A.CUSTR_NBR,
                    CASE
                       WHEN   ROUND (
                                 AVG (
                                    CASE
                                       WHEN SPAY.OPEN_BAL <= 10 THEN 100
                                       ELSE SPAY.PAYMENT / SPAY.OPEN_BAL
                                    END),
                                 4)
                            * 100 > 100
                       THEN
                          100
                       ELSE
                            ROUND (
                               AVG (
                                  CASE
                                     WHEN SPAY.OPEN_BAL <= 10 THEN 100
                                     ELSE SPAY.PAYMENT / SPAY.OPEN_BAL
                                  END),
                               4)
                          * 100
                    END
                       STMTRepayRatio_L12M,
                    CASE
                       WHEN SUM (
                               CASE
                                  WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                  ELSE SPAY.OPEN_BAL
                               END) = 0
                       THEN
                          100
                       ELSE
                            ROUND (
                                 SUM (
                                    CASE
                                       WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                       ELSE SPAY.PAYMENT
                                    END)
                               / SUM (
                                    CASE
                                       WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                       ELSE SPAY.OPEN_BAL
                                    END),
                               4)
                          * 100
                    END
                       STMTRepayAveRatio_L12M
               FROM V_FDM_S24_CUR_ACCT_COM_SP a, MDM_STMT_X spay
              WHERE     todate(spay.STMT_DATE,'yyyy-mm-dd') >= ADD_MONTHS (sysdate(), -12)
                    AND todate(spay.STMT_DATE,'yyyy-mm-dd') < sysdate()
                    AND A.XACCOUNT = SPAY.XACCOUNT
           GROUP BY A.CUSTR_NBR) m12
             ON m12.CUSTR_NBR = ACCT.CUSTR_NBR
          LEFT JOIN
          (  SELECT A.CUSTR_NBR CUSTR_NBR,
                    CASE
                       WHEN SUM (
                               CASE
                                  WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                  ELSE SPAY.OPEN_BAL
                               END) = 0
                       THEN
                          100
                       ELSE
                            ROUND (
                                 SUM (
                                    CASE
                                       WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                       ELSE SPAY.PAYMENT
                                    END)
                               / SUM (
                                    CASE
                                       WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                       ELSE SPAY.OPEN_BAL
                                    END),
                               4)
                          * 100
                    END
                       last3monavgrate
               FROM V_FDM_S24_CUR_ACCT_COM_SP a, MDM_STMT_X spay
              WHERE     todate(spay.STMT_DATE,'yyyy-mm-dd') >= ADD_MONTHS (sysdate(), -3)
                    AND todate(spay.STMT_DATE,'yyyy-mm-dd') < sysdate()
                    AND A.XACCOUNT = SPAY.XACCOUNT
           GROUP BY A.CUSTR_NBR) m3
             ON m3.custr_nbr = acct.custr_nbr
          LEFT JOIN
          (  SELECT A.CUSTR_NBR,
                    CASE
                       WHEN SUM (
                               CASE
                                  WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                  ELSE SPAY.OPEN_BAL
                               END) = 0
                       THEN
                          100
                       ELSE
                            ROUND (
                                 SUM (
                                    CASE
                                       WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                       ELSE SPAY.PAYMENT
                                    END)
                               / SUM (
                                    CASE
                                       WHEN SPAY.OPEN_BAL <= 10 THEN 0
                                       ELSE SPAY.OPEN_BAL
                                    END),
                               4)
                          * 100
                    END
                       STMTRepayRatio_L6M,
                    CASE
                       WHEN SUM (SPAY.LIMIT) = 0
                       THEN
                          0
                       ELSE
                          CASE
                             WHEN   ROUND (
                                         AVG (Point.SHOPPING_POINT)
                                       / SUM (SPAY.LIMIT),
                                       4)
                                  * 100 > 100
                             THEN
                                100
                             ELSE
                                  ROUND (
                                       AVG (Point.SHOPPING_POINT)
                                     / SUM (SPAY.LIMIT),
                                     4)
                                * 100
                          END
                    END
                       STMTUtility_Ave_L6M,
                    AVG (Point.SHOPPING_POINT) points_6m
               FROM V_FDM_S24_CUR_ACCT_COM_SP a 
                    inner join MDM_STMT_X spay on A.XACCOUNT = SPAY.XACCOUNT
                    left join 
                    (  SELECT P.ACCOUNT AS XACCOUNT,
                              SUM (
                                 CASE
                                    WHEN     P.PTLG_TYPE >= '10'
                                         AND P.PTLG_TYPE <= '19'
                                    THEN
                                       BILL_AMT
                                    ELSE
                                       0
                                 END)
                                 AS SHOPPING_POINT
                         FROM FDM_S24_PTLOG P
                        WHERE     todate(P.CREATE_DATE,'yyyy-mm-dd') >= ADD_MONTHS (sysdate(), -6)
                              AND todate(P.CREATE_DATE,'yyyy-mm-dd') < sysdate()
                     GROUP BY P.ACCOUNT) Point on A.XACCOUNT = Point.XACCOUNT
              WHERE     spay.STMT_DATE >= ADD_MONTHS (sysdate(), -6)
                    AND spay.STMT_DATE < sysdate()
           GROUP BY A.CUSTR_NBR) m6
             ON m6.CUSTR_NBR = acct.CUSTR_NBR
          LEFT JOIN
          (  SELECT ACCT.CUSTR_NBR CUSTR_NBR,
                    MAX (stmt.month_count + 0) month_count
               FROM V_FDM_S24_CUR_ACCT_COM_SP acct
                    INNER JOIN
                    (  SELECT S.XACCOUNT xaccount, COUNT (1) month_count
                         FROM MDM_STMT_X s
                        WHERE     s.BAL_ORINT > 0
                              AND todate(s.STMT_DATE,'yyyy-mm-dd') >= ADD_MONTHS (sysdate(), -6)
                              AND todate(S.STMT_DATE,'yyyy-mm-dd') < sysdate()
                     GROUP BY S.XACCOUNT) stmt
                       ON stmt.xaccount = ACCT.XACCOUNT
           GROUP BY ACCT.CUSTR_NBR) bal_m6
             ON ACCT.CUSTR_NBR = bal_m6.CUSTR_NBR
          LEFT JOIN (  SELECT CUSTR_NBR, MAX (PROD_LEVEL + 0) maxproductline
                         FROM V_FDM_S24_CUR_ACCT_COM_SP
                     GROUP BY CUSTR_NBR) a
             ON A.CUSTR_NBR = ACCT.CUSTR_NBR;
