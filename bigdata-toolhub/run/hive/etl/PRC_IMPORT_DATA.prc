INCLUDE /home/hadoop/ccam-bigdata/bigdata-toolhub/run/hive/etl/PRC_IMPORT_DATA_ROLLBACK.prc;
USE db_name;


CREATE PROCEDURE getT_FDM_TABLENAME
  DYNAMIC RESULT SETS 1
BEGIN
  DECLARE c CURSOR WITH RETURN FOR
    SELECT FDM_TABLE,GROUP_ID,CUR_DATA_TABLE,SOURCE_TABLE,KEY_COL,TYPEID,LAST_DATE FROM t_fdm_tablename;
  OPEN c;
END;


CREATE PROCEDURE PRC_IMPORT_DATA(IN db STRING, IN edate STRING)
BEGIN
  DECLARE fdm_table  STRING;
  DECLARE group_id   STRING;
  DECLARE cur_table  STRING;
  DECLARE s24_table  STRING;
  DECLARE key_col    STRING;
  DECLARE type_id    STRING;
  DECLARE last_date  STRING;
  
  DECLARE vidx   INT; 
  DECLARE vstr   STRING;
  DECLARE vjoin  STRING;
  
  DECLARE vhql1  STRING;
  DECLARE vhql2  STRING;
  DECLARE vhql3  STRING;
  DECLARE vhql4  STRING;
  DECLARE vnul   STRING;

  DECLARE pdate  STRING;
  
  DBMS_OUTPUT.PUT_LINE('#');
  DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_DATA.prc running!');
  DBMS_OUTPUT.PUT_LINE('# database name: '||db);
  DBMS_OUTPUT.PUT_LINE('# etl date: '||edate);
  DBMS_OUTPUT.PUT_LINE('#');
  
  CALL getT_FDM_TABLENAME;
  ALLOCATE c1 CURSOR FOR PROCEDURE getT_FDM_TABLENAME;
  
  FETCH c1 INTO fdm_table, group_id, cur_table, s24_table, key_col, type_id, last_date;
  WHILE SQLCODE = 0 DO
    DBMS_OUTPUT.PUT_LINE('# [当前处理行]: '||fdm_table||' - '||group_id||' - '||cur_table||' - '||s24_table||' - '||key_col||' - '||type_id||' - '||last_date);

    IF type_id = 'ZIPPER' THEN
      DBMS_OUTPUT.PUT_LINE('# [当前历史表] fdm_table : '||fdm_table);
 	  DBMS_OUTPUT.PUT_LINE('# [当前主键列表] key_col : '||key_col);

      IF edate <= last_date THEN
	    DBMS_OUTPUT.PUT_LINE('# edate <= last_date, 调用PRC_IMPORT_DATA_ROLLBACK(db, edate) ...');
	    PRC_IMPORT_DATA_ROLLBACK(db, edate);
	  END IF;
	  
      vidx := INSTR(key_col, ',');
      IF vidx > 0 THEN
        vjoin := ' ';
        WHILE vidx > 0 LOOP
          vidx := INSTR(key_col, ',');
          EXIT WHEN vidx = 0;
          vstr := SUBSTR(key_col, 1, vidx - 1);
          key_col := SUBSTR(key_col, vidx + 1);
          vjoin := vjoin||'c.'||vstr||'=s.'||vstr||' AND ';
          vnul := key_col;
        END LOOP;
        vjoin := vjoin||'c.'||key_col||'=s.'||key_col;
      ELSE
        vjoin := 'c.'||key_col||'=s.'||key_col;
        vnul := key_col;
      END IF;

     
      DBMS_OUTPUT.PUT_LINE('# [当前连接条件] vjoin   : '||vjoin);
      DBMS_OUTPUT.PUT_LINE('# [当前末主键]   vnul    : '||vnul);

	  
      -- 第1步  将 cur_table 中与 s24_table 主键[可能多个]相同的数据以ETL日期分区的形式插入 fdm_table
      -- INSERT INTO 替换 INSERT OVERWRITE ???
	  DBMS_OUTPUT.PUT_LINE('# Setp1 ...');
      vhql1 := 'INSERT OVERWRITE TABLE '||db||'.'||fdm_table||' PARTITION (end_date='||edate||') SELECT c.* FROM '||db||'.'||cur_table||' c ';
      vhql1 := vhql1||' INNER JOIN '||db||'.'||s24_table||' s ON '||vjoin;
      DBMS_OUTPUT.PUT_LINE('# vhql1: '||vhql1);
      EXECUTE IMMEDIATE vhql1;
     
      -- 第2步  将 cur_table 中与 s24_table 主键[可能多个]相同的数据删掉
	  DBMS_OUTPUT.PUT_LINE('# Setp2 ...');
      vhql2 := 'INSERT OVERWRITE TABLE '||db||'.'||cur_table||' SELECT c.* FROM '||db||'.'||cur_table||' c ';
      vhql2 := vhql2||' LEFT JOIN '||db||'.'||s24_table||' s WHERE '||vjoin||' AND s.'||vnul||' IS NULL ';
    --vhql2 := vhql2||' LEFT JOIN '||db||'.'||s24_table||' s ON '||vjoin||' WHERE s.'||vnul||' IS NULL ';
      DBMS_OUTPUT.PUT_LINE('# vhql2: '||vhql2);	  
      EXECUTE IMMEDIATE vhql2;
     
      -- 第3步  将 s24_table 中的数据插入 cur_table [多一个ETL日期]
      -- INSERT INTO时SELECT查询为空会报空指针错误!
	  DBMS_OUTPUT.PUT_LINE('# Setp3 ...');
      vhql3 := 'INSERT INTO TABLE '||db||'.'||cur_table||' SELECT '||edate||', s.* FROM '||db||'.'||s24_table||' s ';
      DBMS_OUTPUT.PUT_LINE('# vhql3: '||vhql3);
      EXECUTE IMMEDIATE vhql3;
     
      -- 第4步  若"ETL日期" > "t_fdm_tablename表LAST_DATE字段日期", 则将"LAST_DATE字段日期"更新为"ETL日期"
      -- 注意: 字段的值需要包起来!!!否则报错执行失败!!!
      -- ' '' ' 显示为1个单引号!!!(注意单引号的嵌套转义规则)
 	  IF edate > last_date THEN
	     DBMS_OUTPUT.PUT_LINE('# Setp4: edate > last_date ...');
         vhql4 := 'INSERT OVERWRITE TABLE '||db||'.t_fdm_tablename SELECT FDM_TABLE,GROUP_ID,CUR_DATA_TABLE,SOURCE_TABLE,KEY_COL,TYPEID, ';
         vhql4 := vhql4||' (CASE WHEN FDM_TABLE = TRIM('' '||fdm_table||' '') THEN '||edate||' ELSE LAST_DATE END) FROM '||db||'.t_fdm_tablename ';
         DBMS_OUTPUT.PUT_LINE('# vhql4: '||vhql4);
         EXECUTE IMMEDIATE vhql4;
      END IF;
     
      DBMS_OUTPUT.PUT_LINE('# [当前 ZIPPER 行处理完成]!  #');
      DBMS_OUTPUT.PUT_LINE('');
    ELSE
      DBMS_OUTPUT.PUT_LINE('# [其他表]: '||fdm_table);
      DBMS_OUTPUT.PUT_LINE('');
    END IF;
    -- 最外层判断结束
    
    FETCH c1 INTO fdm_table,group_id,cur_table,s24_table,key_col,type_id,last_date;
  END WHILE;
  
  CLOSE c1;

END;


CALL PRC_IMPORT_DATA(db_name, etl_date);

DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_DATA.prc finished!');
