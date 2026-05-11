-- 秒杀资格判断脚本：库存、时间窗口、一人一单都在 Redis 中原子完成。
-- 这样秒杀入口不需要访问 MySQL，也不需要分布式锁来保证一人一单。

-- 1. 参数
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local now = tonumber(ARGV[4])

-- 2. Redis 数据
-- 2.1 秒杀库存信息，String 结构。
-- key: seckill:stock:{voucherId}
-- value: {"stock":库存,"beginTime":开始时间戳,"endTime":结束时间戳}
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2 秒杀订单信息，Set 结构，记录买过该优惠券的用户 ID。
-- key: seckill:order:{voucherId}
-- value: userId 集合
local orderKey = 'seckill:order:' .. voucherId

local stockJson = redis.call('get', stockKey)
if (not stockJson) then
    return 1
end

local stockInfo = cjson.decode(stockJson)
local stock = tonumber(stockInfo['stock'])
local beginTime = tonumber(stockInfo['beginTime'])
local endTime = tonumber(stockInfo['endTime'])

-- 3. 判断秒杀是否开始或结束。
if (beginTime ~= nil and now < beginTime) then
    return 3
end
if (endTime ~= nil and now > endTime) then
    return 4
end

-- 4. 判断库存是否充足。Redis 单线程执行 Lua，库存预扣减不会被并发打断。
if (stock <= 0) then
    return 1
end

-- 5. 判断一人一单。Set 中存在 userId，说明该用户已经买过。
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- 6. 用户有下单资格：Redis 预扣库存，并记录该用户已经下单。
stockInfo['stock'] = stock - 1
redis.call('set', stockKey, cjson.encode(stockInfo))
redis.call('sadd', orderKey, userId)

-- 7. 资格判断成功。Java 收到 0 后发送 Kafka 消息，异步扣减 MySQL 库存并生成订单。
return 0
