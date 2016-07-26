INCLUDE /home/hadoop/ccam-bigdata/bigdata-toolhub/run/hive/etl/PRC_IMPORT_DATA_ROLLBACK.prc;
USE DATA_BASE;


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
  
  DECLARE v_idx  INT; 
  DECLARE v_key  STRING;
  DECLARE v_join STRING;

  DECLARE vhql   STRING;  
  DECLARE vhql1  STRING;
  DECLARE vhql2  STRING;
  DECLARE vhql3  STRING;
  DECLARE vhql4  STRING;
  DECLARE vnul   STRING;

  DECLARE cnt     INT = 0;
  DECLARE pdate   STRING;
  DECLARE s24_tbl STRING;


  DBMS_OUTPUT.PUT_LINE('#');
  DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_DATA.prc running ...');
  DBMS_OUTPUT.PUT_LINE('# DATA_BASE: '||db);
  DBMS_OUTPUT.PUT_LINE('# ETL_DATE: '||edate);
  DBMS_OUTPUT.PUT_LINE('#');

  
  CALL getT_FDM_TABLENAME;
  ALLOCATE c1 CURSOR FOR PROCEDURE getT_FDM_TABLENAME;
  
  FETCH c1 INTO fdm_table, group_id, cur_table, s24_table, key_col, type_id, last_date;
  WHILE SQLCODE = 0 DO
    DBMS_OUTPUT.PUT_LINE('# [当前处理行]: '||fdm_table||' - '||group_id||' - '||cur_table||' - '||s24_table||' - '||key_col||' - '||type_id||' - '||last_date);

    IF type_id = 'ZIPPER' THEN
      DBMS_OUTPUT.PUT_LINE('# [当前历史表] fdm_table : '||fdm_table);
 	  DBMS_OUTPUT.PUT_LINE('# [当前主键列表] key_col : '||key_col);

      --理论上,导入全量时不需要ROLLBACK;因为全量是起点,没法儿再往前ROLLBACK
--IF edate <= last_date THEN
--DBMS_OUTPUT.PUT_LINE('# edate <= last_date, 调用PRC_IMPORT_DATA_ROLLBACK(db, edate) ...');
--CALL PRC_IMPORT_DATA_ROLLBACK(db, edate, cur_table);
--END IF;
	  
	  
    --问: 如果当前s24_table为空,是否可跳过不做任何处理 ???

	  
      v_idx := INSTR(key_col, ',');
      IF v_idx > 0 THEN
        v_join := ' ';
        WHILE v_idx > 0 LOOP
          v_idx := INSTR(key_col, ',');
          EXIT WHEN v_idx = 0;
          v_key := SUBSTR(key_col, 1, v_idx - 1);
          key_col := SUBSTR(key_col, v_idx + 1);
          v_join := v_join||'c.'||v_key||'=s.'||v_key||' AND ';
          vnul := key_col;
        END LOOP;
        v_join := v_join||'c.'||key_col||'=s.'||key_col;
      ELSE
        v_join := 'c.'||key_col||'=s.'||key_col;
        vnul := key_col;
      END IF;
     
      DBMS_OUTPUT.PUT_LINE('# [当前连接条件] v_join   : '||v_join);
      DBMS_OUTPUT.PUT_LINE('# [当前末主键]   vnul    : '||vnul);

	  
      --第1步  将cur_table中与s24_table主键[可能多个]相同的数据以ETL日期分区的形式插入fdm_table
	  DBMS_OUTPUT.PUT_LINE('# Setp1 ...');
    --insert into tic.FDM_TABLE value(select v_ETL_DATE,p.* from tic.CUR_DATA_TABLE p inner join tic.SOURCE_TABLE t on v_join );  --v_ETL_DATE := to_date(ETL_DATE,'yyyymmdd');
      vhql1 := 'INSERT OVERWRITE TABLE '||db||'.'||fdm_table||' PARTITION (end_date='||edate||') SELECT c.* FROM '||db||'.'||cur_table||' c ';
      vhql1 := vhql1||' INNER JOIN '||db||'.'||s24_table||' s ON '||v_join;
      DBMS_OUTPUT.PUT_LINE('# vhql1: '||vhql1);
      EXECUTE IMMEDIATE vhql1;
     
      --第2步  将cur_table中与s24_table主键[可能多个]相同的数据删掉
        --P.S. 处理逻辑: 查询下边vhql语句结果是否为空,为空则 TRUNCATE cur_table; 不为空则关联删除(关联s24_table)
	  DBMS_OUTPUT.PUT_LINE('# Setp2 ...');
      DBMS_OUTPUT.PUT_LINE('# Begin cnt = '||cnt);
      vhql := 'SELECT count(*) FROM '||db||'.'||cur_table||' c LEFT JOIN '||db||'.'||s24_table||' s ON '||v_join||' WHERE s.'||vnul||' IS NULL ';
      DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
      EXECUTE IMMEDIATE vhql INTO cnt;
      DBMS_OUTPUT.PUT_LINE('# End cnt = '||cnt);
    --delete from tic.CUR_DATA_TABLE p where exists (select 1 from tic.SOURCE_TABLE t where v_join );
      IF cnt <> 0 THEN
        vhql2 := 'INSERT OVERWRITE TABLE '||db||'.'||cur_table||' SELECT c.* FROM '||db||'.'||cur_table||' c ';
        vhql2 := vhql2||' LEFT JOIN '||db||'.'||s24_table||' s ON '||v_join||' WHERE s.'||vnul||' IS NULL ';
      ELSE
        vhql2 := 'TRUNCATE TABLE '||db||'.'||cur_table;
	  END IF;
      DBMS_OUTPUT.PUT_LINE('# vhql2: '||vhql2);
      EXECUTE IMMEDIATE vhql2;
     
      --第3步  将s24_table中的数据插入cur_table[多一个ETL日期]
        --P.S. 处理逻辑: 查询下边vhql语句结果是否为空,为空则跳过vhql3不做任何处理
	  DBMS_OUTPUT.PUT_LINE('# Setp3 ...');
      DBMS_OUTPUT.PUT_LINE('# Begin cnt = '||cnt);
      vhql := 'SELECT COUNT(*) FROM '||db||'.'||s24_table;
      DBMS_OUTPUT.PUT_LINE('# vhql = '||vhql);
      EXECUTE IMMEDIATE vhql INTO cnt;
      DBMS_OUTPUT.PUT_LINE('# End cnt = '||cnt);
   --insert into tic.CUR_DATA_TABLE value(select v_ETL_DATE,p.* from tic.SOURCE_TABLE p); 
      IF cnt <> 0 THEN
        vhql3 := 'INSERT INTO TABLE '||db||'.'||cur_table||' SELECT '||edate||', s.* FROM '||db||'.'||s24_table||' s ';
        DBMS_OUTPUT.PUT_LINE('# vhql3: '||vhql3);
        EXECUTE IMMEDIATE vhql3;
      END IF;
     
      --第4步  若"ETL日期" > "t_fdm_tablename表LAST_DATE字段日期", 则将"LAST_DATE字段日期"更新为"ETL日期"
        --注意: 字段的值需要包起来!!!否则报错执行失败!!!
          --' '' ' 显示为1个单引号!!!(注意单引号的嵌套转义规则)
 	  IF edate > last_date THEN
	     DBMS_OUTPUT.PUT_LINE('# Setp4: edate > last_date ...');
         vhql4 := 'INSERT OVERWRITE TABLE '||db||'.t_fdm_tablename SELECT FDM_TABLE,GROUP_ID,CUR_DATA_TABLE,SOURCE_TABLE,KEY_COL,TYPEID, ';
         vhql4 := vhql4||' (CASE WHEN FDM_TABLE = TRIM('' '||fdm_table||' '') THEN '||edate||' ELSE LAST_DATE END) FROM '||db||'.t_fdm_tablename ';
         DBMS_OUTPUT.PUT_LINE('# vhql4: '||vhql4);
         EXECUTE IMMEDIATE vhql4;
      END IF;

      --第5步  TRUNCATE 对应S24_XXX表(非V_S24_XXX视图) - 阻止增量数据处理过程被重复加载 #
	  DBMS_OUTPUT.PUT_LINE('# Setp5 ...');
      s24_tbl := SUBSTRING(fdm_table, 5);
      DBMS_OUTPUT.PUT_LINE('# s24_tbl: '||s24_tbl);
      vhql := 'TRUNCATE TABLE '||db||'.'||s24_tbl;
      DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
      EXECUTE IMMEDIATE vhql;
	  
      DBMS_OUTPUT.PUT_LINE('# [当前 ZIPPER 行处理完成]!  #');
      DBMS_OUTPUT.PUT_LINE('');
    ELSE
      DBMS_OUTPUT.PUT_LINE('# [其他表]: '||fdm_table);
      DBMS_OUTPUT.PUT_LINE('');
    END IF;
    --最外层判断结束
    
    FETCH c1 INTO fdm_table,group_id,cur_table,s24_table,key_col,type_id,last_date;
  END WHILE;
  
  CLOSE c1;

END;


CALL PRC_IMPORT_DATA(DATA_BASE, ETL_DATE);

DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_DATA.prc finished !');
DBMS_OUTPUT.PUT_LINE('#');
