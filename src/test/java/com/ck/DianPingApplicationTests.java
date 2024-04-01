package com.ck;

import com.ck.entity.Shop;
import com.ck.service.impl.ShopServiceImpl;
import com.ck.utils.CacheClient;
import com.ck.utils.RedisConstants;
import com.ck.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import static com.ck.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
class DianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    private ExecutorService es = Executors.newFixedThreadPool(500);
    @Test
    void testIdWorker() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }

    @Test
    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
    }

    @Test
    void loadShopData(){
        //1. 查询店铺信息
        List<Shop> list = shopService.list();
        //2. 将店铺分组，按照typeId分组，typeId一致的放到一个集合中
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3. 分批写入到redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 获取店铺id
            Long typeId = entry.getKey();
            String key = RedisConstants.SHOP_GEO_KEY + typeId;
            // 获取同类型店铺的集合
            List<Shop> shops = entry.getValue();

            // 写入redis GEOADD key 经度 纬度 member
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());

            for (Shop shop : shops) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())));
            }

            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

    @Test
    void testPipeline(){

        // 1.建立连接
        Jedis jedis = new Jedis("192.168.200.130", 6379);
        // jedis = JedisConnectionFactory.getJedis();
        // 2.设置密码
        jedis.auth("123321");
        // 3.选择库
        jedis.select(0);

        // 创建管道做批处理
        Pipeline pipeline = jedis.pipelined();

        long start = System.currentTimeMillis();

        for (int i = 1; i < 100000; i++) {
            // 放入命令到管道
            pipeline.set("test:key_" + i, "value_" + i);
            if(i % 1000 == 0){
                // 每放入1000条命令，批量执行
                pipeline.sync();
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("执行时间=" + (end - start) + "ms");
    }

}
