package com.annuus.db.hbase.exception;

/**
 * Hbase exception.
 * User: shuiqing
 * DateTime: 16/4/26 上午11:28
 * Email: annuus.sq@gmail.com
 * GitHub: https://github.com/shuiqing301
 * Blog: http://shuiqing301.github.io/
 * _
 * |_)._ _
 * | o| (_
 */
public class SimpleHBaseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SimpleHBaseException(String message) {
        super(message);
    }

    public SimpleHBaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SimpleHBaseException(Throwable cause) {
        super(cause);
    }
}