package com.annuus.db.hbase.tableinfo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Hbase中的Column.
 * User: shuiqing
 * DateTime: 16/4/26 上午11:32
 * Email: annuus.sq@gmail.com
 * GitHub: https://github.com/shuiqing301
 * Blog: http://shuiqing301.github.io/
 * _
 * |_)._ _
 * | o| (_
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseColumn {
    /**
     * 功能：定义hbase family name
     * @return	column family
     */
    String family() default "";

    /**
     * 功能：定义hbase column qualifier name.
     * @return	qualifier
     */
    String qualifier();

    /**
     * 功能：定义是否以string格式存储到hbase中
     * @return	true：是；false：不是
     */
    boolean isStoreStringType() default false;

    /**
     * 功能：定义
     * @return
     */
    OrderRule defaultOrderRule() default OrderRule.NONE;

    /**
     * 功能：如果toJson为true，则把对象转为json String，然后存入数据库中，读取时把bytes转为String，然后转为对象
     * @return
     */
    boolean toJson() default false;
}