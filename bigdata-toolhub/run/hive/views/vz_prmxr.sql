use zhgl;

CREATE VIEW if not exists V_PRMXR AS
SELECT T.BANK,
          T.CURR_NUM,
          T.EFFECT_DAY,
          T.RATE_VAL0,
          T.RATE_VAL1,
          T.RATE_VAL2,
          T.RATE_VAL3,
          T.RATE_VAL4,
          T.RATE_VAL5,
          T.RATE_VAL6
     FROM V_S24_PRMXR T
          JOIN (  SELECT BANK, CURR_NUM, MAX (EFFECT_DAY) AS EFFECT_DAY
                    FROM V_S24_PRMXR
                GROUP BY BANK, CURR_NUM) A
             ON     T.BANK = A.BANK
                AND T.CURR_NUM = A.CURR_NUM
                AND T.EFFECT_DAY = A.EFFECT_DAY;

