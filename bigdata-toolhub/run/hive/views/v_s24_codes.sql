use zhgl;

CREATE VIEW if not exists V_S24_CODES AS
SELECT BANK,
          CODE_TYPE,
          CODE_VALUE,
          BRIEF,
          VALUE_DEF,
          SHORT_DEF,
          VAR_A1
     FROM S24_CODES;


