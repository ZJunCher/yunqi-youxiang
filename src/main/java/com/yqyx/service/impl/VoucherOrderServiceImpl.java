package com.yqyx.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.yqyx.dto.Result;
import com.yqyx.entity.VoucherOrder;
import com.yqyx.mapper.VoucherOrderMapper;
import com.yqyx.service.ISeckillVoucherService;
import com.yqyx.service.IVoucherOrderService;
import com.yqyx.utils.OrderConstants;
import com.yqyx.utils.RedisIdWorker;
import com.yqyx.utils.SystemConstants;
import com.yqyx.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Resource
    private TransactionTemplate transactionTemplate;

    @Value("${yqyx.kafka.seckill-order-topic}")
    private String seckillOrderTopic;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> RELEASE_STOCK_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        RELEASE_STOCK_SCRIPT = new DefaultRedisScript<>();
        RELEASE_STOCK_SCRIPT.setLocation(new ClassPathResource("release_seckill_stock.lua"));
        RELEASE_STOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        /*
         * 秒杀入口只负责确认用户是否有下单资格：
         * 1. Lua 从 Redis String 中读取库存、开始时间、结束时间；
         * 2. Lua 从 Redis Set 中判断该用户是否已经买过；
         * 3. 如果有资格，Lua 在 Redis 中预扣库存，并把用户 ID 写入 Set。
         *
         * 这一段不查询 MySQL，也不使用分布式锁。
         * 防超卖和一人一单依赖 Redis 执行 Lua 脚本的原子性来保证。
         */
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId),
                String.valueOf(System.currentTimeMillis())
        );
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(getSeckillFailMessage(r));
        }

        /*
         * Redis 已经确认用户有资格下单，此时可以认为抢券成功。
         * MySQL 扣库存和创建订单延迟到 Kafka 消费阶段异步完成。
         */
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        sendSeckillOrderMessage(voucherOrder);
        return Result.ok(orderId);
    }

    @Override
    public Result queryOrderById(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return Result.fail("订单不存在");
        }
        Long userId = UserHolder.getUser().getId();
        if (!userId.equals(order.getUserId())) {
            return Result.fail("无权查看该订单");
        }

        /*
         * 被动关单：
         * 定时任务每分钟执行一次，可能存在订单已经超时但还没被扫描到的短暂窗口。
         * 用户查看订单详情时顺手检查一次，可以让用户看到更及时的订单状态。
         */
        closeIfExpired(order, LocalDateTime.now());
        return Result.ok(getById(orderId));
    }

    @Override
    public Result queryMyOrders(Integer current) {
        Long userId = UserHolder.getUser().getId();

        /*
         * 订单列表的被动关单：
         * 返回列表前先关闭当前用户已经超时的未支付订单，避免继续展示过期的待支付状态。
         */
        closeExpiredOrdersByUser(userId);

        Page<VoucherOrder> page = query()
                .eq("user_id", userId)
                .orderByDesc("create_time")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        return Result.ok(page.getRecords());
    }

    @Override
    public Result handlePaySuccess(Long orderId) {
        /*
         * 支付回调和超时关单会争夺同一个订单状态。
         *
         * 支付成功只有在下面这条 SQL 更新成功时才算处理成功：
         * update tb_voucher_order
         * set status = 已支付, pay_time = 当前时间
         * where id = ? and status = 未支付
         *
         * 超时关单也使用同样的乐观锁思想：
         * update tb_voucher_order
         * set status = 已取消
         * where id = ? and status = 未支付
         *
         * 两条 SQL 都要求 status = 未支付，因此并发时只有一方能成功。
         */
        boolean paid = update()
                .set("status", OrderConstants.STATUS_PAID)
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("status", OrderConstants.STATUS_UNPAID)
                .update();
        if (paid) {
            // 扩展点：这里可以发放优惠券、增加积分，或触发其他支付成功后的业务。
            log.info("订单支付成功，orderId = {}", orderId);
            return Result.ok();
        }

        /*
         * 如果支付状态更新失败，说明订单已经被其他流程改过状态：
         * 1. 已支付：说明是重复支付回调，直接幂等返回成功；
         * 2. 已取消：说明超时关单先成功，库存可能已经释放，此时必须触发退款。
         */
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return Result.fail("订单不存在");
        }
        if (order.getStatus() != null && order.getStatus() == OrderConstants.STATUS_PAID) {
            log.info("重复支付回调，订单已支付，orderId = {}", orderId);
            return Result.ok();
        }
        if (order.getStatus() != null && order.getStatus() == OrderConstants.STATUS_CANCELED) {
            refundCanceledOrder(order);
            return Result.fail("订单已超时取消，已触发退款");
        }
        return Result.fail("订单状态不允许支付");
    }

    @Scheduled(fixedDelay = 60_000)
    public void closeExpiredUnpaidOrders() {
        LocalDateTime expireBefore = LocalDateTime.now().minusMinutes(OrderConstants.ORDER_TIMEOUT_MINUTES);

        /*
         * 主动关单：
         * 每分钟扫描一小批未支付且已超时的订单。
         * LIMIT 100 是为了避免一次任务集中占用过多数据库连接、行锁和 Redis 执行时间。
         * 处理失败的订单状态仍然是未支付，下一轮扫描还会继续重试。
         */
        List<VoucherOrder> orders = query()
                .eq("status", OrderConstants.STATUS_UNPAID)
                .lt("create_time", expireBefore)
                .last("LIMIT 100")
                .list();
        if (orders == null || orders.isEmpty()) {
            return;
        }
        for (VoucherOrder order : orders) {
            closeOrderAndReleaseStock(order);
        }
    }

    private void closeExpiredOrdersByUser(Long userId) {
        LocalDateTime expireBefore = LocalDateTime.now().minusMinutes(OrderConstants.ORDER_TIMEOUT_MINUTES);
        List<VoucherOrder> orders = query()
                .eq("user_id", userId)
                .eq("status", OrderConstants.STATUS_UNPAID)
                .lt("create_time", expireBefore)
                .list();
        for (VoucherOrder order : orders) {
            closeOrderAndReleaseStock(order);
        }
    }

    private boolean closeIfExpired(VoucherOrder order, LocalDateTime now) {
        if (order.getStatus() == null || order.getStatus() != OrderConstants.STATUS_UNPAID) {
            return false;
        }
        if (order.getCreateTime() == null) {
            return false;
        }

        LocalDateTime expireTime = order.getCreateTime().plusMinutes(OrderConstants.ORDER_TIMEOUT_MINUTES);
        if (expireTime.isAfter(now)) {
            return false;
        }
        return closeOrderAndReleaseStock(order);
    }

    private boolean closeOrderAndReleaseStock(VoucherOrder order) {
        try {
            Boolean success = transactionTemplate.execute(status -> {
                /*
                 * 超时关单的乐观锁：
                 * 只有未支付订单才能被更新为已取消。
                 * 如果支付回调已经先把订单改成已支付，这里会更新失败，也就不会释放库存。
                 */
                boolean closed = update()
                        .set("status", OrderConstants.STATUS_CANCELED)
                        .eq("id", order.getId())
                        .eq("status", OrderConstants.STATUS_UNPAID)
                        .update();
                if (!closed) {
                    return false;
                }

                /*
                 * 只有订单状态关闭成功，才释放 MySQL 库存。
                 * 这样可以避免定时任务重复执行时把库存重复加回。
                 */
                boolean mysqlStockReleased = seckillVoucherService.update()
                        .setSql("stock = stock + 1")
                        .eq("voucher_id", order.getVoucherId())
                        .update();
                if (!mysqlStockReleased) {
                    status.setRollbackOnly();
                    return false;
                }

                /*
                 * Redis 补偿：
                 * 1. Redis 秒杀库存加回；
                 * 2. 从 seckill:order:{voucherId} 中移除用户 ID。
                 * 如果不移除用户 ID，用户后续会被 Lua 判断为重复下单，无法再次抢券。
                 */
                releaseRedisStock(order);
                return true;
            });
            return Boolean.TRUE.equals(success);
        } catch (Exception e) {
            /*
             * 关单失败时不把订单吞掉。
             * 它仍然保持未支付状态，后续定时任务或被动关单还能继续重试。
             */
            log.error("关闭超时订单失败，orderId = {}", order.getId(), e);
            return false;
        }
    }

    private void releaseRedisStock(VoucherOrder order) {
        stringRedisTemplate.execute(
                RELEASE_STOCK_SCRIPT,
                Collections.emptyList(),
                order.getVoucherId().toString(),
                order.getUserId().toString()
        );
    }

    private void refundCanceledOrder(VoucherOrder order) {
        /*
         * 真实接入支付系统时，这里应调用第三方支付的原路退款接口。
         * 订单已经进入已取消这个终态，并且库存已经释放，所以不能再把订单改回已支付。
         */
        log.warn("订单已取消但收到支付成功回调，需要原路退款，orderId = {}", order.getId());
    }

    private String getSeckillFailMessage(int result) {
        switch (result) {
            case 1:
                return "库存不足";
            case 2:
                return "不能重复下单";
            case 3:
                return "秒杀尚未开始";
            case 4:
                return "秒杀已经结束";
            default:
                return "秒杀失败";
        }
    }

    private void sendSeckillOrderMessage(VoucherOrder voucherOrder) {
        String message = JSONUtil.toJsonStr(voucherOrder);
        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(seckillOrderTopic, voucherOrder.getId().toString(), message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("kafka sendMessage error, topic = {}, data = {}", seckillOrderTopic, message, ex);
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.debug("kafka sendMessage success, topic = {}, data = {}", seckillOrderTopic, message);
            }
        });
    }

    @KafkaListener(topics = "${yqyx.kafka.seckill-order-topic}")
    public void processSeckillOrder(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            VoucherOrder voucherOrder = JSONUtil.toBean(record.value(), VoucherOrder.class);
            createVoucherOrder(voucherOrder);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("consume seckill order error, topic = {}, offset = {}, data = {}",
                    record.topic(), record.offset(), record.value(), e);
        }
    }

    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        /*
         * Kafka 可能重复投递消息。
         * Redis Lua 已经保证业务上的一人一单，这里的数据库查重主要是消费端幂等兜底。
         */
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.warn("重复消费或重复订单消息，userId = {}, voucherId = {}", userId, voucherId);
            return;
        }

        Boolean success = transactionTemplate.execute(status -> {
            /*
             * MySQL 扣库存和创建订单放在同一个事务里：
             * 如果库存扣减失败，不保存订单；
             * 如果订单保存失败，库存扣减也会回滚。
             */
            boolean stockDeducted = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!stockDeducted) {
                return false;
            }

            voucherOrder.setStatus(OrderConstants.STATUS_UNPAID);
            voucherOrder.setCreateTime(LocalDateTime.now());
            return save(voucherOrder);
        });

        if (!Boolean.TRUE.equals(success)) {
            log.error("创建秒杀订单失败，userId = {}, voucherId = {}", userId, voucherId);
        }
    }
}
