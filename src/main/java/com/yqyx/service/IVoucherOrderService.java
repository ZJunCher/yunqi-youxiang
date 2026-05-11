package com.yqyx.service;

import com.yqyx.dto.Result;
import com.yqyx.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result queryOrderById(Long orderId);

    Result queryMyOrders(Integer current);

    Result handlePaySuccess(Long orderId);
}
