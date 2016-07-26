USE DATA_BASE;


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

  DECLARE v_idx        INT;
  DECLARE v_key        STRING;
  DECLARE v_partitions STRING;
  
  DECLARE cnt     INT = 0;
  DECLARE pdate   STRING;
  DECLARE s24_tbl STRING;

  
  DBMS_OUTPUT.PUT_LINE('#');
  DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_LOG_DATA.prc running ...');
  DBMS_OUTPUT.PUT_LINE('# DATA_BASE: '||db);
  DBMS_OUTPUT.PUT_LINE('# ETL_DATE: '||edate);
  DBMS_OUTPUT.PUT_LINE('#');

  
  CALL getT_FDM_TABLENAME;
  ALLOCATE c1 CURSOR FOR PROCEDURE getT_FDM_TABLENAME;
  
  FETCH c1 INTO fdm_table, group_id, cur_table, s24_table, key_col, type_id, last_date;
  WHILE SQLCODE = 0 DO
    DBMS_OUTPUT.PUT_LINE('# [当前处理行]: '||fdm_table||' - '||group_id||' - '||cur_table||' - '||s24_table||' - '||key_col||' - '||type_id||' - '||last_date);
   
    IF type_id = 'LOG' THEN
      DBMS_OUTPUT.PUT_LINE('# [当前历史表] fdm_table : '||fdm_table);
	  DBMS_OUTPUT.PUT_LINE('# [当前S24视图] s24_table : '||s24_table);
	  
      --查询当前s24_table是否为空,为空则跳过不做任何处理
	  DBMS_OUTPUT.PUT_LINE('# Begin cnt = '||cnt);
	  vhql := 'SELECT COUNT(*) FROM '||db||'.'||s24_table;
      DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
	  EXECUTE IMMEDIATE vhql INTO cnt;
	  DBMS_OUTPUT.PUT_LINE('# End cnt = '||cnt);

      IF cnt <> 0 THEN
        --处理逻辑: 1)获取分区日期pdate[可能存在多个] 2)删除fdm_table中pdate对应的分区
	    IF key_col = ' ' THEN
          --传入的edate可以保证是8位yyyymmdd日期格式,故这里不再显示转换
		  v_partitions := 'PARTITION (create_date='||edate||')';
        ELSE
          --拼接分区表删除条件 v_partitions
		  --P.S. 以逗号','分隔主键的方式因为"todate(date_add(add_months(todate('19900101','yyyy-mm-dd'),p.MONTH_NBR-2),p.CYCLE_NBR-1),'yyyymmdd')"的特殊存在而失败!
          --v_idx := INSTR(key_col, ',');
          --IF v_idx > 0 THEN
          --  WHILE v_idx > 0 LOOP
          --    v_idx := INSTR(key_col, ',');
          --    EXIT WHEN v_idx = 0;
          --    v_key := SUBSTR(key_col, 1, v_idx - 1);
          --    key_col := SUBSTR(key_col, v_idx + 1);
          --    --错误的用法: 因为Spark中可能没有"zhgl.todate"这个函数,而存在函数"todate"
          --    --vhql := 'SELECT '||db||'.todate('||v_key||',''yyyymmdd'') FROM '||db||'.'||s24_table;
          --    vhql := 'SELECT todate('||v_key||',''yyyymmdd'') FROM '||db||'.'||s24_table;
          --    DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
          --    EXECUTE IMMEDIATE vhql INTO pdate;
          --    v_partitions := 'PARTITION (create_date='||pdate||'), ';
          --  END LOOP;
          --  vhql := 'SELECT todate('||key_col||',''yyyymmdd'') FROM '||db||'.'||s24_table;
          --  DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
          --  EXECUTE IMMEDIATE vhql INTO pdate;
          --  v_partitions := v_partitions||'PARTITION (create_date='||pdate||')';
          --ELSE
          --  --经测试,不指定"LIMIT 1"时查询结果也为第一行的key_col日期值,使用"LIMIT 1"以期加快执行速度
          --  vhql := 'SELECT todate('||key_col||',''yyyymmdd'') FROM '||db||'.'||s24_table||' LIMIT 1 ';
          --  DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
          --  EXECUTE IMMEDIATE vhql INTO pdate;
          --  v_partitions := 'PARTITION (create_date='||pdate||')';
          --END IF;

          vhql := 'SELECT todate('||key_col||',''yyyymmdd'') FROM '||db||'.'||s24_table||' p LIMIT 1 ';
          DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
          EXECUTE IMMEDIATE vhql INTO pdate;		  
		  v_partitions := 'PARTITION (create_date='||pdate||')';
        END IF;
        DBMS_OUTPUT.PUT_LINE('# [当前分区表删除条件] v_partitions: '||v_partitions);
		
        --delete from tic.FDM_TABLE t where t.create_date=v_KEY_COL;  --v_KEY_COL := to_date(ETL_DATE,'yyyymmdd');
        --delete from tic.FDM_TABLE t where t.create_date in (select distinct v_KEY_COL from tic.SOURCE_TABLE p );  --v_KEY_COL := to_date(tic.KEY_COL,'yyyymmdd');
        vhql1 := 'ALTER TABLE '||db||'.'||fdm_table||' DROP IF EXISTS '||v_partitions||' PURGE ';
		  
        --insert into tic.FDM_TABLE value(select v_KEY_COL , p.* from tic.SOURCE_TABLE p );
	    vhql2 := 'INSERT OVERWRITE TABLE '||db||'.'||fdm_table||' '||v_partitions||' SELECT p.* FROM '||db||'.'||s24_table||' p ';

        --第1步  若"ETL日期" <= "t_fdm_tablename表LAST_DATE字段日期", 则删掉fdm_table中create_date=pdate的分区数据[可能存在多个这样的分区]
        IF edate <= last_date THEN
		  DBMS_OUTPUT.PUT_LINE('# Setp1: edate <= last_date ...');
	      DBMS_OUTPUT.PUT_LINE('# vhql1: '||vhql1);
          EXECUTE IMMEDIATE vhql1;
        END IF;
		
        --第2步  将s24_table的数据以日期分区的形式插入fdm_table
          --P.S. 若第1步删除分区时pdate对应多个分区,那么对应的第2步以分区形式插入数据时理论上也要根据pdate创建多个不同的分区分别插入数据 - TODO
		DBMS_OUTPUT.PUT_LINE('# Setp2 ...');
	    DBMS_OUTPUT.PUT_LINE('# vhql2: '||vhql2);
        EXECUTE IMMEDIATE vhql2;       
		
        --第3步  若"ETL日期" > "t_fdm_tablename表LAST_DATE字段日期", 则将"LAST_DATE字段日期"更新为"ETL日期"
        IF edate > last_date THEN
		  DBMS_OUTPUT.PUT_LINE('# Setp3: edate > last_date ...');
          vhql3 := 'INSERT OVERWRITE TABLE '||db||'.t_fdm_tablename SELECT FDM_TABLE,GROUP_ID,CUR_DATA_TABLE,SOURCE_TABLE,KEY_COL,TYPEID,';
 	      vhql3 := vhql3||' (CASE WHEN FDM_TABLE = TRIM('' '||fdm_table||' '') THEN '||edate||' ELSE LAST_DATE END) FROM '||db||'.t_fdm_tablename ';
          DBMS_OUTPUT.PUT_LINE('# vhql3: '||vhql3);
          EXECUTE IMMEDIATE vhql3;
        END IF;
		
        --第4步  TRUNCATE 对应S24_XXX表(非V_S24_XXX视图) - 阻止增量数据处理过程被重复加载 #
		DBMS_OUTPUT.PUT_LINE('# Setp4 ...');
        s24_tbl := SUBSTRING(fdm_table, 5);
        DBMS_OUTPUT.PUT_LINE('# s24_tbl: '||s24_tbl);
        vhql := 'TRUNCATE TABLE '||db||'.'||s24_tbl;
        DBMS_OUTPUT.PUT_LINE('# vhql: '||vhql);
        EXECUTE IMMEDIATE vhql;
	  
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


CALL PRC_IMPORT_LOG_DATA(DATA_BASE, ETL_DATE);

DBMS_OUTPUT.PUT_LINE('# PRC_IMPORT_LOG_DATA.prc finished !');
DBMS_OUTPUT.PUT_LINE('#');
