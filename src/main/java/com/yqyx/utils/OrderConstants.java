package com.yqyx.utils;

public class OrderConstants {
    private OrderConstants() {
    }

    /**
     * 订单已创建但还没有支付。超时关单只处理这个状态。
     */
    public static final int STATUS_UNPAID = 1;

    /**
     * 已支付订单不能被超时关单任务关闭，也不能释放库存。
     */
    public static final int STATUS_PAID = 2;

    public static final int STATUS_USED = 3;

    /**
     * 超时未支付订单会被更新为已取消。
     */
    public static final int STATUS_CANCELED = 4;

    public static final int STATUS_REFUNDING = 5;
    public static final int STATUS_REFUNDED = 6;

    /**
     * 用户下单后超过这个时间仍未支付，就会被主动或被动关单。
     */
    public static final int ORDER_TIMEOUT_MINUTES = 15;
}
