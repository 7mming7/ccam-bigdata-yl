use zhgl;

CREATE VIEW if not exists MAPPING_CODES AS
SELECT BANK,
          CODE_TYPE,
          CODE_VALUE,
          BRIEF,
          VALUE_DEF,
          SHORT_DEF,
          VAR_A1,
          CODE_VALUE AS CODE_VALUE_DS,
          BRIEF AS BRIEF_DS,
          VALUE_DEF AS VALUE_DEF_DS,
          SHORT_DEF AS SHORT_DEF_DS,
          VAR_A1 AS VAR_A1_DS
     FROM S24_CODES;

