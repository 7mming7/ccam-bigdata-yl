USE db_name;


CREATE PROCEDURE getT_FDM_TABLENAME
  DYNAMIC RESULT SETS 1
BEGIN
  DECLARE c CURSOR WITH RETURN FOR
    SELECT FDM_TABLE,GROUP_ID,CUR_DATA_TABLE,SOURCE_TABLE,KEY_COL,TYPEID,LAST_DATE FROM t_fdm_tablename;
  OPEN c;
END;


CREATE PROCEDURE PRC_IMPORT_LOG_DATA(IN db STRING, IN edate STRING)
BEGIN
  DECLARE fdm_table  STRING;
  DECLARE group_id   STRING;
  DECLARE cur_table  STRING;
  DECLARE s24_table  STRING;
  DECLARE key_col    STRING;
  DECLARE type_id    STRING;
  DECLARE last_date  STRING;

  DECLARE vhql   STRING;  
  DECLARE vhql1  STRING;
  DECLARE vhql2  STRING;
  DECLARE vhql3  STRING;

  DECLARE cnt     INT = 0;
  DECLARE pdate   STRING;

  
  DBMS_OUTPUT.PUT_LINE('#');
  DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_LOG_DATA.prc running!');
  DBMS_OUTPUT.PUT_LINE('# Database Name: '||db);
  DBMS_OUTPUT.PUT_LINE('# ETL Date: '||edate);
  DBMS_OUTPUT.PUT_LINE('#');

  
  CALL getT_FDM_TABLENAME;
  ALLOCATE c1 CURSOR FOR PROCEDURE getT_FDM_TABLENAME;
  
  FETCH c1 INTO fdm_table, group_id, cur_table, s24_table, key_col, type_id, last_date;
  WHILE SQLCODE = 0 DO
    DBMS_OUTPUT.PUT_LINE('# [当前处理行]: '||fdm_table||' - '||group_id||' - '||cur_table||' - '||s24_table||' - '||key_col||' - '||type_id||' - '||last_date);
   
    IF type_id = 'LOG' THEN
      DBMS_OUTPUT.PUT_LINE('# [当前历史表] fdm_table : '||fdm_table);
	  DBMS_OUTPUT.PUT_LINE('# [当前S24视图] s24_table : '||s24_table);

	  pdate := '';  -- 注意,需要显示赋空值!!!
	  
      -- 查询当前 s24_table 是否为空,为空则跳过不做任何处理
	  DBMS_OUTPUT.PUT_LINE('# Beging cnt = '||cnt);
	  vhql := 'SELECT COUNT(*) FROM '||db||'.'||s24_table;
      DBMS_OUTPUT.PUT_LINE('# vhql = '||vhql);
	  EXECUTE IMMEDIATE vhql INTO cnt;
	  DBMS_OUTPUT.PUT_LINE('# End cnt = '||cnt);

      IF cnt <> 0 THEN
	    IF key_col = ' ' THEN
          SET pdate = edate;
        --SELECT toDate(edate,'yyyymmdd') INTO pdate FROM s24_table;  -- 执行报错!
	  	  vhql1 := 'INSERT OVERWRITE TABLE '||db||'.'||fdm_table||' SELECT f.* FROM '||db||'.'||fdm_table||' f WHERE f.create_date <> '||pdate;					
        ELSE
        --SELECT toDate(key_col,'yyyymmdd') INTO pdate FROM db||'.'||s24_table;	
          vhql := 'SELECT toDate('||key_col||',''yyyymmdd'') FROM '||db||'.'||s24_table||' p LIMIT 1';
        --vhql := 'SELECT toDate('||key_col||',''yyyymmdd'') FROM '||db||'.'||s24_table||' p ';
	      EXECUTE IMMEDIATE vhql INTO pdate;		  
          vhql1 := 'INSERT OVERWRITE TABLE '||db||'.'||fdm_table||' SELECT f.* FROM '||db||'.'||fdm_table||' f ';
          vhql1 := vhql1||' LEFT JOIN '||db||'.'||s24_table||' s WHERE f.create_date = '||pdate||' AND '||pdate||' IS NULL ';	  
        END IF;

	    vhql2 := 'INSERT OVERWRITE TABLE '||db||'.'||fdm_table||' PARTITION (create_date='||pdate||') SELECT p.* FROM '||db||'.'||s24_table||' p ';        	    

        -- 第1步  若"ETL日期" <= "t_fdm_tablename表LAST_DATE字段日期", 则删掉 fdm_table 中 create_date=pdate 的分区数据
        IF edate <= last_date THEN
		  DBMS_OUTPUT.PUT_LINE('# Setp1: edate <= last_date ...');
	      DBMS_OUTPUT.PUT_LINE('# vhql1: '||vhql1);
          EXECUTE IMMEDIATE vhql1;
        END IF;

		
        -- 第2步  将 s24_table 的数据以日期分区的形式插入 fdm_table
		DBMS_OUTPUT.PUT_LINE('# Setp2 ...');
	    DBMS_OUTPUT.PUT_LINE('# vhql2: '||vhql2);
        EXECUTE IMMEDIATE vhql2;
        
		
        -- 第3步  若"ETL日期" > "t_fdm_tablename表LAST_DATE字段日期", 则将"LAST_DATE字段日期"更新为"ETL日期"
        IF edate > last_date THEN
		  DBMS_OUTPUT.PUT_LINE('# Setp3: edate > last_date ...');
          vhql3 := 'INSERT OVERWRITE TABLE '||db||'.t_fdm_tablename SELECT FDM_TABLE,GROUP_ID,CUR_DATA_TABLE,SOURCE_TABLE,KEY_COL,TYPEID,';
 	      vhql3 := vhql3||' (CASE WHEN FDM_TABLE = TRIM('' '||fdm_table||' '') THEN '||edate||' ELSE LAST_DATE END) FROM '||db||'.t_fdm_tablename ';
          DBMS_OUTPUT.PUT_LINE('# vhql3: '||vhql3);
          EXECUTE IMMEDIATE vhql3;
        END IF;
      END IF;
      DBMS_OUTPUT.PUT_LINE('# [当前 LOG 行处理完成]!  #');
      DBMS_OUTPUT.PUT_LINE('#');
    ELSE
      DBMS_OUTPUT.PUT_LINE('# [其他表]: '||fdm_table);
      DBMS_OUTPUT.PUT_LINE('#');
    END IF;
    
    FETCH c1 INTO fdm_table,group_id,cur_table,s24_table,key_col,type_id,last_date;
  END WHILE;
  
  CLOSE c1;

END;


CALL PRC_IMPORT_LOG_DATA(db_name, etl_date);

DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_LOG_DATA.prc finished!');
