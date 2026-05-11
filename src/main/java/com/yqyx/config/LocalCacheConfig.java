package com.yqyx.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.yqyx.entity.Shop;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class LocalCacheConfig {

    @Bean
    public Cache<Long, Shop> shopLocalCache() {
        /*
         * 店铺详情本地缓存：
         * 1. 本地缓存放在应用进程内，访问速度比 Redis 更快；
         * 2. 设置较短 TTL，避免集群场景下本地缓存长时间不一致；
         * 3. 设置最大容量，避免热点数据过多导致应用内存无限增长。
         */
        return Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .build();
    }
}
