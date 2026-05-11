package com.yqyx.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.yqyx.dto.Result;
import com.yqyx.entity.SeckillVoucher;
import com.yqyx.entity.Voucher;
import com.yqyx.mapper.VoucherMapper;
import com.yqyx.service.ISeckillVoucherService;
import com.yqyx.service.IVoucherService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yqyx.utils.RedisConstants.SECKILL_STOCK_KEY;

@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        // 1. 保存优惠券基础信息到 MySQL。
        save(voucher);

        // 2. 保存秒杀券信息到 MySQL，作为最终落库数据。
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);

        // 3. 将秒杀资格判断需要的数据提前写入 Redis。
        // key: seckill:stock:{voucherId}
        // value: {"stock":库存,"beginTime":开始时间戳,"endTime":结束时间戳}
        // 秒杀入口只访问 Redis，由 Lua 完成库存、时间窗口和一人一单判断，避免 MySQL 承载高并发查询。
        Map<String, Object> stockInfo = new HashMap<>();
        stockInfo.put("stock", voucher.getStock());
        stockInfo.put("beginTime", voucher.getBeginTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        stockInfo.put("endTime", voucher.getEndTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        stringRedisTemplate.opsForValue().set(SECKILL_STOCK_KEY + voucher.getId(), JSONUtil.toJsonStr(stockInfo));
    }
}
