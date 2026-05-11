package com.yqyx.controller;


import com.yqyx.dto.Result;
import com.yqyx.limiter.annotation.RateLimiter;
import com.yqyx.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("seckill/{id}")
    @RateLimiter(
            key = "coupon:seckill:",
            window = 10,
            limit = 100,
            message = "秒杀活动太火爆，请稍后再试",
            type = RateLimiter.LimitType.GLOBAL
    )
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    @GetMapping("/{id}")
    public Result queryOrderById(@PathVariable("id") Long orderId) {
        return voucherOrderService.queryOrderById(orderId);
    }

    @GetMapping("/of/me")
    public Result queryMyOrders(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        return voucherOrderService.queryMyOrders(current);
    }

    @PostMapping("/pay/success/{id}")
    public Result handlePaySuccess(@PathVariable("id") Long orderId) {
        return voucherOrderService.handlePaySuccess(orderId);
    }
}
