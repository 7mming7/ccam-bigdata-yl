use zhgl;

CREATE VIEW if not exists V_MAIN_ACCT AS
SELECT ta.XACCOUNT, ta.CUSTR_NBR, ta.CATEGORY
  FROM (SELECT T.XACCOUNT,
               T.CUSTR_NBR,
               T.CATEGORY,
               ROW_NUMBER()
               OVER(PARTITION BY t.custr_nbr ORDER BY t.LAST_trday + 0,t.PROD_LEVEL + 0,t.CRED_LIMIT + 0,t.XACCOUNT + 0 DESC) cn
          FROM fdm_s24_cur_acct t
          JOIN (SELECT MAX(tb.PROD_LEVEL) prod_level, tb.CUSTR_NBR
                 FROM (select t.PROD_LEVEL, t.CUSTR_NBR, t.PROD_NBR
                         from fdm_s24_cur_acct t
                         left join (select C.code_value, C.bank_id
                                     from ccam_codes C, tbl_banks B
                                    where B.bankid = C.bank_id
                                      and C.code_type = 'TSZH') t1
                           on t.CATEGORY = t1.code_value
                         left join (select C.code_value, C.bank_id
                                     from ccam_codes C, tbl_banks B
                                    where B.bankid = C.bank_id
                                      and C.code_type = 'TSCP') t2
                           on t.PROD_NBR = t2.code_value
                        where t.ACCT_STS NOT IN ('Q', 'W', 'WQ','')
                          and t1.bank_id is null
                          and t2.bank_id is null) tb
                GROUP BY tb.CUSTR_NBR) maxLEVEL
            ON t.CUSTR_NBR = maxLEVEL.CUSTR_NBR
           AND t.prod_level = maxLEVEL.prod_level
            left join (select C.code_value, C.bank_id
                                     from ccam_codes C, tbl_banks B
                                    where B.bankid = C.bank_id
                                      and C.code_type = 'TSZH') t3
                           on t.CATEGORY = t3.code_value
                         left join (select C.code_value, C.bank_id
                                     from ccam_codes C, tbl_banks B
                                    where B.bankid = C.bank_id
                                      and C.code_type = 'TSCP') t4
                           on t.PROD_NBR = t4.code_value
                        where t3.bank_id is null
                          and t4.bank_id is null ) ta
 WHERE ta.cn = 1;