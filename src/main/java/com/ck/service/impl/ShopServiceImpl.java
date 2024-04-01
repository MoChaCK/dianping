package com.ck.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ck.dto.Result;
import com.ck.entity.Shop;
import com.ck.mapper.ShopMapper;
import com.ck.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ck.utils.CacheClient;
import com.ck.utils.RedisConstants;
import com.ck.utils.RedisData;
import com.ck.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ck.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    /**
     * 添加商铺信息缓存
     */
    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透的方法
        //Shop shop = queryWithPassThrough(id);
//        Shop shop = cacheClient.queryWithPassThrough(
//                CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient
                .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if(shop == null){
            return Result.fail("店铺不存在！");
        }

        // 返回
        return Result.ok(shop);
    }


//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    *
//     * 逻辑过期解决缓存击穿的逻辑
//     * 主要解决热点key的问题
//     * @return
//
//    public Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        //1. 从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2. 判断是否存在
//        if(StrUtil.isBlank(shopJson)){
//            //2.1 缓存未命中，直接返回空
//            return null;
//        }
//
//        //2.2 缓存命中，先把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject)redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //3. 判断缓存是否过期
//        if(expireTime.isAfter(LocalDateTime.now())){
//            //3.1 未过期，返回商铺信息
//            return shop;
//        }
//        //3.2 缓存过期，需要缓存重建
//        //4. 缓存重建
//        //4.1 尝试获取互斥锁
//        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
//        //4.2 判断是否获取成功
//        if(isLock){
//
//            // 获取锁成功，再次检测redis缓存是否过期，做 DoubleCheck，如果存在则无需缓存重建
//            shopJson = stringRedisTemplate.opsForValue().get(key);
//            if(StrUtil.isBlank(shopJson)){
//                return null;
//            }
//            redisData = JSONUtil.toBean(shopJson, RedisData.class);
//            shop = JSONUtil.toBean((JSONObject)redisData.getData(), Shop.class);
//            expireTime = redisData.getExpireTime();
//            if(expireTime.isAfter(LocalDateTime.now())){
//                return shop;
//            }
//
//            //4.3 成功，开启独立线程，实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    // 重建缓存
//                    this.saveShop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    // 释放锁
//                    unlock(LOCK_SHOP_KEY + id);
//                }
//            });
//        }
//
//        //4.4 无论获取成功与否，都返回过期的商铺信息
//        return shop;
//    }


    /**
     * 互斥锁解决缓存击穿的逻辑
     * @return
     */
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1. 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3. 缓存命中，直接返回商铺信息
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 解决缓存穿透，命中空值，说明店铺不存在，返回错误信息
        if(shopJson != null){ //说明 shopJson = ""，空字符串，店铺不存在
            return null;
        }

        Shop shop = null;
        try {
            //4. 未命中缓存，实现缓存重建
            //4.1 尝试获取互斥锁
            boolean flag = tryLock(LOCK_SHOP_KEY + id);
            //4.2 判断是否获取成功
            if(!flag){
                //4.3 没获取到，休眠一段时间，并重试
                Thread.sleep(50);
                queryWithMutex(id);
            }
            //4.4 获取到锁，再次检测redis缓存是否存在，做 DoubleCheck，如果缓存存在了，无需缓存重建
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isNotBlank(shopJson)){
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            if(shopJson != null){
                return null;
            }
            //4.5 根据id从数据库中查询
            shop = getById(id);
            // 模拟重建的延时
            Thread.sleep(200);
            //5.1 查询数据库的商铺不存在
            if(shop == null){
                // 被动解决缓存穿透，商铺不存在，就存入空值
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }
            //5.2  商铺存在，将商铺数据写入到redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //6. 释放互斥锁
            unlock(LOCK_SHOP_KEY + id);
        }

        //7. 返回商铺信息
        return shop;
    }



//    /**
//     * 解决缓存穿透的逻辑
//     * @return
//     */
//    public Shop queryWithPassThrough(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        //1. 从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2. 判断是否存在
//        if(StrUtil.isNotBlank(shopJson)){
//            //3. 缓存命中，直接返回商铺信息
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 解决缓存穿透，命中空值，说明店铺不存在，返回错误信息
//        if(shopJson != null){ //说明 shopJson = ""，空字符串，店铺不存在
//            return null;
//        }
//        //4. 未命中，根据id从数据库中查询
//        Shop shop = getById(id);
//        //5.1 数据库的商铺不存在，返回404错误
//        if(shop == null){
//            // 被动解决缓存穿透，商铺不存在，就存入空值
//            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
//            // 返回错误信息
//            return null;
//        }
//        //5.2 商铺存在，将商铺数据写入到redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        //6. 返回商铺信息
//        return shop;
//    }


    /**
     * 利用redis中的setnx()方法实现获取锁
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", CACHE_NULL_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    /**
     * 删除锁
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 1. 预热，将热点数据提前加入到缓存中
     * 2. 缓存重建
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //1. 查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //2. 加上逻辑过期时间，封装店铺信息
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3. 将封装好的店铺信息保存到redis中
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存更新：更改数据库的信息，并删除缓存，实现数据的一致性
     * @param shop
     * @return
     */
    @Override
    @Transactional //保证事务
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        //1. 更改数据库信息
        updateById(shop);
        //2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if(x == null || y == null){
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query().eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from = (current - 1) *  SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis，按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH Key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        //4.解析出id
        if(results == null){
            return Result.ok();
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from){
            // 没有下一页了，直接结束
            return Result.ok(Collections.emptyList());
        }
        //4.1 截取from - end 的部分
        ArrayList<Long> ids = new ArrayList<>(list.size());
        HashMap<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            //4.2 获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        //5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6.返回
        return Result.ok(shops);
    }
}
