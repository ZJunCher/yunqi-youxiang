-- 关闭超时未支付订单后，释放 Redis 侧的秒杀资格数据。
-- 这里用 Lua 一次性完成两件事：
-- 1. seckill:stock:{voucherId} 中的库存加回；
-- 2. seckill:order:{voucherId} Set 中移除 userId。
-- 如果只加库存但不移除 userId，用户再次抢券时仍会被判断为重复下单。

local voucherId = ARGV[1]
local userId = ARGV[2]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 库存信息是 JSON 字符串，格式由 VoucherServiceImpl 写入：
-- {"stock":库存,"beginTime":开始时间戳,"endTime":结束时间戳}
local stockJson = redis.call('get', stockKey)
if (stockJson) then
    local stockInfo = cjson.decode(stockJson)
    local stock = tonumber(stockInfo['stock'])
    if (stock == nil) then
        stock = 0
    end
    stockInfo['stock'] = stock + 1
    redis.call('set', stockKey, cjson.encode(stockInfo))
end

-- 移除用户已购记录，允许该用户在订单取消后重新参与抢券。
redis.call('srem', orderKey, userId)
return 1
