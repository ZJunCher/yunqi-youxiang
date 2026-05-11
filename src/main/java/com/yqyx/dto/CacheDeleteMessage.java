package com.yqyx.dto;

import lombok.Data;

@Data
public class CacheDeleteMessage {

    /**
     * 需要删除的 Redis key。
     */
    private String key;

    /**
     * 当前已经重试的次数，用于超过阈值后触发告警日志。
     */
    private Integer retryTimes;
}
