USE db;

CREATE OR REPLACE PROCEDURE PRC_IMPORT_DATA_ROLLBACK (IN db STRING, IN edate STRING, IN cur_table STRING)
BEGIN

DECLARE  v_sql          STRING;
DECLARE  FDM_TABLE      STRING;
DECLARE  v_join         STRING;
DECLARE  v_cols         STRING;
DECLARE  v_last       INT;
DECLARE  v_strs_last    STRING;
DECLARE  v_idx          INT;
DECLARE  v_str          STRING;
DECLARE  tic            STRING;
DECLARE  data_type      STRING;
DECLARE  comment            STRING;
DECLARE  FDM_TABLE      STRING;
DECLARE  SOURCE_TABLE   STRING;
DECLARE  KEY_COL        STRING;
DECLARE  Last_KEY_COL   STRING;

DBMS_OUTPUT.PUT_LINE('#');
DBMS_OUTPUT.PUT_LINE('# Database Name: '||db);
DBMS_OUTPUT.PUT_LINE('# ETL Date: '||edate);
DBMS_OUTPUT.PUT_LINE('#');

 DECLARE table_column_cr CURSOR FOR 'desc '||db||'.'|| cur_table;                                       
 OPEN table_column_cr;
 FETCH table_column_cr  INTO tic,data_type,comment;
 WHILE SQLCODE=0 THEN
   v_cols := v_cols || ' p.' || tic|| ',';
   FETCH table_column_cr INTO tic;
   END WHILE;
 CLOSE table_column_cr;

 v_last :=length(v_cols)-1;
 v_cols :=SUBSTR(v_cols,0,v_last);

 DECLARE table_info_cr CURSOR FOR
   'SELECT FDM_TABLE, SOURCE_TABLE, KEY_COL FROM '||db||'.T_FDM_TABLENAME WHERE CUR_DATA_TABLE =TRIM('' '||cur_table||''' )';
   OPEN table_info_cr;
     FETCH table_info_cr  INTO FDM_TABLE,SOURCE_TABLE,KEY_COL;
       WHILE SQLCODE=0 THEN

         v_strs_last := KEY_COL;
         v_idx := INSTR (v_strs_last, ',');
         FDM_TABLE := FDM_TABLE;
         IF (v_idx > 0)
         THEN
           v_join := ' ';
  
           WHILE vidx > 0 LOOP
             v_idx := INSTR (v_strs_last, ',');
           EXIT WHEN v_idx = 0;
             v_str := SUBSTR (v_strs_last, 1, v_idx - 1);
             v_strs_last := SUBSTR (v_strs_last, v_idx + 1);
             v_join := v_join || 'T.' || v_str || '=P.' || v_str || ' and ';
           END LOOP;
  
           Last_KEY_COL := v_strs_last;
           v_join := v_join || 'T.' || v_strs_last || '=P.' || v_strs_last;
         ELSE
           v_join := 'T.' || v_strs_last || '=P.' || v_strs_last;
         END IF;
   
         Last_KEY_COL := v_strs_last;
         DBMS_OUTPUT.PUT_LINE('# [当前表主键]   Last_KEY_COL    : '||Last_KEY_COL);
		 DBMS_OUTPUT.PUT_LINE('# [表连接条件]   v_join    : '||v_join);
  
         v_sql := 'INSERT OVERWRITE TABLE '||db||'.'||cur_table||' SELECT p.* FROM '||db||'.'||cur_table||' p ';
         v_sql := v_sql ||' LEFT JOIN '||db||'.' ||FDM_TABLE|| ' t ON '|| v_join || ' and '
         ||'t.end_date=todate('|| edate|| ', ''yyyymmdd'')'
         || ' where t.'||Last_KEY_COL||' is null' ;
         DBMS_OUTPUT.put_line (v_sql);
         EXECUTE IMMEDIATE v_sql;
  
         v_sql := 'insert into '
         ||db||'.'
         || cur_table
         || ' select '
         || v_cols
         || ' from '
         ||db||'.'
         || FDM_TABLE
         || ' p inner join '
         ||db||'.'
         || SOURCE_TABLE
         || ' t on '
         || v_join
         || ' where p.end_date=todate('''
         || edate
         || ',''yyyymmdd'') ';
  
         DBMS_OUTPUT.put_line (v_sql);
         EXECUTE IMMEDIATE v_sql;
		 
		 v_sql := ' ALTER TABLE ' ||db||'.'||FDM_TABLE|| ' DROP PARTITION (end_date='||edate||') ';
		 DBMS_OUTPUT.put_line (v_sql);
		 EXECUTE IMMEDIATE v_sql;
  
        FETCH table_info_cr  INTO FDM_TABLE,SOURCE_TABLE,KEY_COL;
  
       END WHILE;
  
   CLOSE table_info_cr;

END;

--CALL PRC_IMPORT_DATA_ROLLBACK(dataBase, etlDate, curTableName);
