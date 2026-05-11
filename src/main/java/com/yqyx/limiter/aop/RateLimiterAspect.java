package com.yqyx.limiter.aop;

import com.yqyx.dto.UserDTO;
import com.yqyx.limiter.annotation.RateLimiter;
import com.yqyx.limiter.exception.RateLimitException;
import com.yqyx.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.UUID;

@Slf4j
@Aspect
@Component
public class RateLimiterAspect {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SLIDING_WINDOW_SCRIPT;

    static {
        SLIDING_WINDOW_SCRIPT = new DefaultRedisScript<>();
        SLIDING_WINDOW_SCRIPT.setLocation(new ClassPathResource("limiter.lua"));
        SLIDING_WINDOW_SCRIPT.setResultType(Long.class);
    }

    @Before("@annotation(rateLimiter)")
    public void doBefore(JoinPoint point, RateLimiter rateLimiter) {
        String key = buildRateLimitKey(point, rateLimiter);
        long now = System.currentTimeMillis();
        String member = now + "-" + UUID.randomUUID();

        /*
         * 所有限流判断都交给 Redis Lua 一次性完成：
         * 1. 删除时间窗口外的旧请求；
         * 2. 统计当前窗口内请求数；
         * 3. 未超过阈值则写入本次请求，超过阈值则返回 0。
         * Lua 脚本在 Redis 中原子执行，可以避免并发计数不准。
         */
        Long result = stringRedisTemplate.execute(
                SLIDING_WINDOW_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(rateLimiter.window()),
                String.valueOf(rateLimiter.limit()),
                String.valueOf(now),
                member
        );
        if (result == null || result == 0) {
            throw new RateLimitException(rateLimiter.message());
        }
    }

    private String buildRateLimitKey(JoinPoint point, RateLimiter rateLimiter) {
        MethodSignature signature = (MethodSignature) point.getSignature();
        Method method = signature.getMethod();

        /*
         * 限流维度的本质就是 Redis key 的不同。
         * 同一个接口在不同维度下会拼出不同 key，从而得到不同的限流粒度。
         */
        StringBuilder keyBuilder = new StringBuilder(rateLimiter.key())
                .append(method.getDeclaringClass().getName())
                .append(":")
                .append(method.getName());

        switch (rateLimiter.type()) {
            case USER:
                keyBuilder.append(":user:").append(getCurrentUserId());
                break;
            case IP:
                keyBuilder.append(":ip:").append(getClientIp());
                break;
            case GLOBAL:
            default:
                keyBuilder.append(":global");
                break;
        }
        return keyBuilder.toString();
    }

    private String getCurrentUserId() {
        UserDTO user = UserHolder.getUser();
        if (user == null || user.getId() == null) {
            return "anonymous";
        }
        return user.getId().toString();
    }

    private String getClientIp() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return "unknown";
        }
        HttpServletRequest request = attributes.getRequest();
        String ip = request.getHeader("X-Forwarded-For");
        if (isInvalidIp(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (isInvalidIp(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (isInvalidIp(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }

    private boolean isInvalidIp(String ip) {
        return ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip);
    }
}
