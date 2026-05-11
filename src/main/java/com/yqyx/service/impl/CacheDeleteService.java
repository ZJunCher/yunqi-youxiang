package com.yqyx.service.impl;

import cn.hutool.json.JSONUtil;
import com.yqyx.dto.CacheDeleteMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service
public class CacheDeleteService {

    private static final int MAX_RETRY_TIMES = 5;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${yqyx.kafka.cache-delete-topic}")
    private String cacheDeleteTopic;

    public void deleteOrSendRetry(String key) {
        try {
            /*
             * 更新 MySQL 后优先同步删除缓存。
             * key 不存在时 delete 返回 false，这种情况可以认为目标已经达成，不需要补偿。
             */
            stringRedisTemplate.delete(key);
        } catch (Exception e) {
            /*
             * 删除缓存失败时，把要删除的 key 投递到 Kafka。
             * 后续由消费者继续重试删除，尽量保证 Redis 和 MySQL 最终一致。
             */
            log.error("删除缓存失败，发送 Kafka 补偿消息，key = {}", key, e);
            sendRetryMessage(key, 0);
        }
    }

    @KafkaListener(topics = "${yqyx.kafka.cache-delete-topic}")
    public void consumeCacheDeleteMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String key = null;
        Integer retryTimes = 0;
        try {
            CacheDeleteMessage message = JSONUtil.toBean(record.value(), CacheDeleteMessage.class);
            key = message.getKey();
            retryTimes = message.getRetryTimes() == null ? 0 : message.getRetryTimes();
            /*
             * 消费补偿消息时再次删除缓存。
             * 只有删除逻辑执行完成后才 ack，避免消息在处理过程中丢失。
             */
            stringRedisTemplate.delete(key);
            ack.acknowledge();
        } catch (Exception e) {
            /*
             * 删除仍然失败时，带着新的重试次数重新投递。
             * 达到最大重试次数后记录告警日志，并 ack 当前消息，避免同一条消息无限阻塞消费。
             */
            int nextRetryTimes = retryTimes + 1;
            if (nextRetryTimes > MAX_RETRY_TIMES) {
                log.error("缓存删除补偿达到最大重试次数，请人工介入排查，key = {}, record = {}",
                        key, record.value(), e);
                ack.acknowledge();
                return;
            }
            log.warn("缓存删除补偿失败，准备第 {} 次重试，key = {}", nextRetryTimes, key, e);
            if (key != null) {
                sendRetryMessage(key, nextRetryTimes);
            }
            ack.acknowledge();
        }
    }

    private void sendRetryMessage(String key, Integer retryTimes) {
        CacheDeleteMessage message = new CacheDeleteMessage();
        message.setKey(key);
        message.setRetryTimes(retryTimes);
        kafkaTemplate.send(cacheDeleteTopic, key, JSONUtil.toJsonStr(message));
    }
}
