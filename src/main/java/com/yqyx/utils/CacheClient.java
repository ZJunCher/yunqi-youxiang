package com.yqyx.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.yqyx.utils.RedisConstants.CACHE_NULL_TTL;
import static com.yqyx.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        /*
         * 逻辑过期时间放在 value 内部，用来判断是否需要异步重建缓存。
         * 同时给 Redis key 设置更长的物理 TTL，作为缓存删除失败时的兜底清理。
         */
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        long logicalSeconds = unit.toSeconds(time);
        long physicalSeconds = logicalSeconds + TimeUnit.DAYS.toSeconds(1);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData), physicalSeconds, TimeUnit.SECONDS);
    }

    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }

        R r = dbFallback.apply(id);
        if (r == null) {
            /*
             * 缓存空值防穿透：
             * 数据库不存在的数据写入短 TTL 空字符串，避免相同无效 id 反复打到 MySQL。
             */
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        this.set(key, r, time, unit);
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (json == null) {
            return null;
        }
        /*
         * 空字符串是缓存空值，不是逻辑过期数据。
         * 这里直接返回 null，调用方需要在查 MySQL 前先识别空值并返回失败。
         */
        if (json.isEmpty()) {
            return null;
        }

        RedisData redisData = parseRedisData(key, json);
        if (redisData == null || redisData.getData() == null || redisData.getExpireTime() == null) {
            /*
             * 旧版本可能写入过普通 JSON，而不是 RedisData 包装结构。
             * 删除旧格式缓存后返回 null，让调用方重新查 MySQL 并写入新格式缓存。
             */
            stringRedisTemplate.delete(key);
            return null;
        }

        R r = convertRedisData(redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }

        /*
         * 逻辑过期后只让一个线程重建缓存。
         * 抢不到锁的请求直接返回旧数据，保证热点 key 过期时服务仍然可用。
         */
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R newR = dbFallback.apply(id);
                    if (newR == null) {
                        stringRedisTemplate.delete(key);
                        return;
                    }
                    this.setWithLogicalExpire(key, newR, time, unit);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        return r;
    }

    private RedisData parseRedisData(String key, String json) {
        try {
            return JSONUtil.toBean(json, RedisData.class);
        } catch (Exception e) {
            log.warn("逻辑过期缓存解析失败，准备删除旧缓存，key = {}", key, e);
            return null;
        }
    }

    private <R> R convertRedisData(Object data, Class<R> type) {
        if (data instanceof JSONObject) {
            return JSONUtil.toBean((JSONObject) data, type);
        }
        return JSONUtil.toBean(JSONUtil.toJsonStr(data), type);
    }

    public <R, ID> R queryWithMutex(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }

        String lockKey = LOCK_SHOP_KEY + id;
        R r;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            r = dbFallback.apply(id);
            if (r == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
