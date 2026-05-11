package com.yqyx.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.benmanes.caffeine.cache.Cache;
import com.yqyx.dto.Result;
import com.yqyx.entity.Shop;
import com.yqyx.mapper.ShopMapper;
import com.yqyx.service.IShopService;
import com.yqyx.utils.CacheClient;
import com.yqyx.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.yqyx.utils.RedisConstants.CACHE_NULL_TTL;
import static com.yqyx.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.yqyx.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.yqyx.utils.RedisConstants.SHOP_GEO_KEY;

@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    @Resource
    private CacheDeleteService cacheDeleteService;
    @Resource
    private Cache<Long, Shop> shopLocalCache;

    @Override
    public Result queryById(Long id) {
        /*
         * 店铺详情使用 Caffeine + Redis + MySQL 二级缓存：
         * 1. 先查本地缓存，命中后不访问 Redis；
         * 2. 本地未命中，再查 Redis；
         * 3. Redis 命中正常数据后回填本地缓存；
         * 4. Redis 没有有效数据时，最后才查 MySQL。
         */
        Shop localShop = shopLocalCache.getIfPresent(id);
        if (localShop != null) {
            return Result.ok(localShop);
        }

        String key = CACHE_SHOP_KEY + id;
        String redisValue = stringRedisTemplate.opsForValue().get(key);
        /*
         * 缓存空值防穿透：
         * Redis 中的空字符串表示 MySQL 也不存在该店铺。
         * 命中空值时直接返回，不能继续查 MySQL，否则缓存穿透保护会失效。
         */
        if (redisValue != null && redisValue.isEmpty()) {
            return Result.fail("店铺不存在！");
        }

        /*
         * Redis 层使用逻辑过期防缓存击穿。
         * 若缓存逻辑过期，会先返回旧数据，并由抢到锁的线程异步重建缓存。
         */
        Shop redisShop = cacheClient.queryWithLogicalExpire(
                CACHE_SHOP_KEY,
                id,
                Shop.class,
                this::getById,
                CACHE_SHOP_TTL,
                TimeUnit.MINUTES
        );
        if (redisShop != null) {
            shopLocalCache.put(id, redisShop);
            return Result.ok(redisShop);
        }

        /*
         * Redis 不存在有效数据时才查 MySQL。
         * 这通常发生在热点数据未预热、缓存被删除，或物理 TTL 到期之后。
         */
        Shop dbShop = getById(id);
        if (dbShop == null) {
            /*
             * MySQL 也不存在时写入短 TTL 空字符串。
             * 后续相同无效 id 会直接命中 Redis 空值，不再访问数据库。
             */
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return Result.fail("店铺不存在！");
        }

        cacheClient.setWithLogicalExpire(key, dbShop, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        shopLocalCache.put(id, dbShop);
        return Result.ok(dbShop);
    }

    @Override
    public Result saveShopToRedis(Long id, Long expireSeconds) {
        Shop shop = getById(id);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        /*
         * 手动预热热点店铺缓存。
         * 活动或压测开始前可调用该接口，把数据提前写入 Redis 和本地缓存。
         */
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + id, shop, expireSeconds, TimeUnit.SECONDS);
        shopLocalCache.put(id, shop);
        return Result.ok();
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        updateById(shop);

        /*
         * 更新 MySQL 后再删除缓存，但删除动作必须等事务提交成功后执行。
         * 如果事务最终回滚，提前删缓存可能导致旧数据又被查出并写回 Redis。
         */
        deleteCacheAfterCommit(id);
        return Result.ok();
    }

    private void deleteCacheAfterCommit(Long id) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    deleteShopCache(id);
                }
            });
            return;
        }

        // 没有事务上下文时直接删除，保证该方法在非事务场景复用时仍然生效。
        deleteShopCache(id);
    }

    private void deleteShopCache(Long id) {
        /*
         * 本地缓存只影响当前应用实例，直接删除即可。
         * Redis 删除失败时由 CacheDeleteService 发送 Kafka 补偿消息继续重试。
         */
        shopLocalCache.invalidate(id);
        cacheDeleteService.deleteOrSendRetry(CACHE_SHOP_KEY + id);
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if (x == null || y == null) {
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }

        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            distanceMap.put(shopIdStr, result.getDistance());
        });

        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
