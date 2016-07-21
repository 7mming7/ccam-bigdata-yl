use zhgl;

CREATE VIEW if not exists V_PRMXR_H AS
SELECT /*+ MAPJOIN(T)*/ T.BANK,
            T.CURR_NUM,
            T.EFFECT_DAY AS ST_DT,
            MIN (COALESCE (A.EFFECT_DAY, 30001231)) AS END_DT,
            T.RATE_VAL0,
            T.RATE_VAL1,
            T.RATE_VAL2,
            T.RATE_VAL3,
            T.RATE_VAL4,
            T.RATE_VAL5,
            T.RATE_VAL6
       FROM V_S24_PRMXR T
            JOIN V_S24_PRMXR A
               ON     T.BANK = A.BANK
                  AND T.CURR_NUM = A.CURR_NUM
                  AND T.EFFECT_DAY < A.EFFECT_DAY
   GROUP BY T.BANK,
            T.CURR_NUM,
            T.EFFECT_DAY,
            T.RATE_VAL0,
            T.RATE_VAL1,
            T.RATE_VAL2,
            T.RATE_VAL3,
            T.RATE_VAL4,
            T.RATE_VAL5,
            T.RATE_VAL6;
