use zhgl;

CREATE VIEW if not exists V_S24_CHGS_ADJ AS
SELECT T1.XACCOUNT XACCOUNT,
          CASE
             WHEN T1.ENTRY_CODE IN ('AC02') THEN '1'                  
             WHEN T1.ENTRY_CODE IN ('ACB2', 'AC97', 'ACB3') THEN '2' 
             ELSE '1'                                              
          END
             AS LIMIT_ADJ_TYPE,
          CASE
             WHEN     T1.ENTRY_CODE IN ('AC02')
                  AND   (nvl(T1.NEW_VALU, 0)
                      - nvl(T1.PRIOR_VALU, 0)) >= 0
             THEN
                '1'                                       
             WHEN     T1.ENTRY_CODE IN ('AC02')
                  AND   (nvl(T1.NEW_VALU, 0)
                      - nvl(T1.PRIOR_VALU, 0)) < 0
             THEN
                '2'                                      
             ELSE
                '0'                                   
          END
             AS LIMIT_ADJ_SIGN,
          CASE
             WHEN T1.ENTRY_TYPE = 'BCREDA' THEN '2'               
             WHEN T1.ENTRY_TYPE IN ('CUCRM', 'CUTRM', 'MBKQ0') THEN '1' 
             ELSE '1'                               
          END
             AS HISTUPDATEDTYPE,
          T1.ENTRY_DAY
     FROM S24_CHGS T1
    WHERE T1.ENTRY_CODE IN ('AC02',
                            'ACB2',
                            'AC97',
                            'ACB3')
;

