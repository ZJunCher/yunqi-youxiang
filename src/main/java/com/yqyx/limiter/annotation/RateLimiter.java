package com.yqyx.limiter.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimiter {

    /**
     * 限流 key 前缀。
     * 最终 Redis key 会在这个前缀后追加方法名和限流维度信息。
     */
    String key() default "rate_limit:";

    /**
     * 滑动时间窗口大小，单位为秒。
     */
    int window() default 10;

    /**
     * 当前时间窗口内允许的最大请求数。
     */
    int limit() default 20;

    /**
     * 请求被限流时返回给前端的提示信息。
     */
    String message() default "系统繁忙，请稍后再试";

    /**
     * 限流维度。
     */
    LimitType type() default LimitType.GLOBAL;

    enum LimitType {
        /**
         * 全局限流：同一个接口共享一个限流计数器。
         */
        GLOBAL,

        /**
         * 用户限流：同一个接口下，每个登录用户各自独立限流。
         */
        USER,

        /**
         * IP 限流：同一个接口下，每个客户端 IP 各自独立限流。
         */
        IP
    }
}
