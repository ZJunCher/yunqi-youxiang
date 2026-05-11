local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

-- 参数不完整时直接返回 Redis 错误，方便尽早发现接入问题。
if not key or not window or not limit or not now or not member then
    return redis.error_reply("Invalid input parameters")
end

-- Java 传入的窗口单位是秒，脚本内部统一换算成毫秒。
local windowMillis = window * 1000

-- 删除窗口外的请求记录，只保留 [now - window, now] 范围内的数据。
redis.call('ZREMRANGEBYSCORE', key, 0, now - windowMillis)

-- 删除旧数据后，ZSet 剩余元素数量就是当前滑动窗口内的请求数量。
local current = redis.call('ZCARD', key)

if current < limit then
    -- score 使用当前毫秒时间戳，member 使用唯一值，避免同一毫秒内请求互相覆盖。
    redis.call('ZADD', key, now, member)
    -- key 的过期时间略大于窗口时间，避免长期遗留冷门限流 key。
    redis.call('EXPIRE', key, window + 1)
    return current + 1
else
    -- 返回 0 表示本次请求被限流。
    return 0
end
