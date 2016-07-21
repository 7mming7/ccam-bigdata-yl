package com.annuus.db.hbase.tableinfo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * HbaseTable注解，定义了tableName和familyName
 * User: shuiqing
 * DateTime: 16/4/26 上午11:31
 * Email: annuus.sq@gmail.com
 * GitHub: https://github.com/shuiqing301
 * Blog: http://shuiqing301.github.io/
 * _
 * |_)._ _
 * | o| (_
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseTable {
    /**
     * 功能：定义表名
     * @return
     */
    String tableName()  default "";

    /**
     * 功能：定义column family name
     * @return
     */
    String defaultFamily() default "";
}
