package com.ck.service.impl;

import com.ck.dto.Result;
import com.ck.entity.VoucherOrder;
import com.ck.mapper.VoucherOrderMapper;
import com.ck.service.ISeckillVoucherService;
import com.ck.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ck.utils.RedisIdWorker;
import com.ck.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker; // 全局id生成器

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    // 线程池
    public static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /*@PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new voucherOrderHandler());
    }

    private class voucherOrderHandler implements Runnable{
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true){
                try {
                    //1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2. 判断消息是获取成功
                    //2.1 获取失败，说明没有消息，继续下一次循环
                    if(list == null || list.isEmpty()){
                        continue;
                    }
                    //3. 解析消息中的订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4. 获取成功可以下单
                    handleVoucherOrder(voucherOrder);

                    //5. ACK确认 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }

            }
        }
        // 处理pending-list中的消息
        private void handlePendingList() {
            while (true){
                try {
                    //1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0")));

                    //2. 判断消息是获取成功
                    //2.1 获取失败，说明pending-list中没有消息，退出循环
                    if(list == null || list.isEmpty()){
                        break;
                    }
                    //3. 解析消息中的订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4. 获取成功可以下单
                    handleVoucherOrder(voucherOrder);

                    //5. ACK确认 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }

                }

            }
        }
    }*/

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class voucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    //1. 获取阻塞队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2. 创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }

            }
        }
    }*/


    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {

        //3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 获取用户id
        Long userId = UserHolder.getUser().getId();
        // 订单id
        long orderId = redisIdWorker.nextId("order");


        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));

        //2.判断结果是否为0
        int r = result.intValue();
        //2.1 不为0，代表没有购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不可重复下单");
        }

        //4. 返回订单id
        return Result.ok(orderId);
    }


    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId, userId);

        //2.判断结果是否为0
        int r = result.intValue();
        //2.1 不为0，代表没有购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不可重复下单");
        }
        //2.2 为0，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        //2.3 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.4 用户id
        voucherOrder.setUserId(userId);
        //2.5 代金券id
        voucherOrder.setVoucherId(voucherId);
        //2.6 放入阻塞队列
        orderTasks.add(voucherOrder);
        //3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3. 返回订单id
        return Result.ok(orderId);
    }*/


    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 获取用户
        Long userId = voucherOrder.getUserId();
        // 2. 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 3. 获取锁
        boolean isLock = lock.tryLock();
        //4. 判断获取锁是否成功
        if(!isLock){
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
/*    @Override
    public Result seckillVoucher(Long voucherId) {

        //1. 查询优惠券信息
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        LocalDateTime endTime = seckillVoucher.getEndTime();
        Integer stock = seckillVoucher.getStock();
        //2. 判断秒杀是否开始
        if(beginTime.isAfter(LocalDateTime.now())){
            // 秒杀未开始
            return Result.fail("秒杀未开始！");
        }
        //3. 判断秒杀是否结束
        if(endTime.isBefore(LocalDateTime.now())){
            // 秒杀已经结束
            return Result.fail("秒杀已结束！");
        }

        //4. 判断库存是否充足
        if(stock < 1){
            return Result.fail("库存不足！");
        }

        Long userId = UserHolder.getUser().getId();
        // 加锁，只需加在用户id上，锁同一个用户即可，同一个用户抢一把锁
        // intern()返回常量池中的地址，保证了同一个用户的字符串对象返回的地址都相等
        // 这里锁的范围是createVoucherOrder()整个方法，保证了方法可以先提交事务再释放锁
//        synchronized (userId.toString().intern()){
//            //获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }

        //使用redis分布式锁
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);

        // 获取锁（可重入），指定锁的名称
        RLock lock = redissonClient.getLock("order:" + userId);

        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁
        if(!isLock){
            //获取锁失败，返回错误或重试
            return Result.fail("一人只能下一单");
        }
        try {
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            lock.unlock();
        }


    }*/

    @Transactional //事务只需要加在操作数据库的方法上即可
    public void createVoucherOrder(VoucherOrder voucherOrder) {

        //5. 判断一人一单
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次！");
            return;
        }

        //6. 库存充足，修改数据库，减库存，操作数据库
        boolean success = seckillVoucherService.update().eq("voucher_id", voucherOrder.getVoucherId()).setSql("stock = stock - 1").update();
        if (!success) {
            // 扣减失败
            log.error("库存不足");
            return;
        }

        //7. 创建订单，操作数据库
        save(voucherOrder);
    }
}
