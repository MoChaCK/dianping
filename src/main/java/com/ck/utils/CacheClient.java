package com.ck.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.ck.utils.RedisConstants.*;

/**
 * 封装缓存的数据库
 * 1. 将任意java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间（普通缓存）
 * 2. 将任意java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题（针对热点缓存）
 * 3. 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题（普通缓存）
 * 4. 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题（针对热点缓存）
 */
@Slf4j
@Component
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 1. 将任意java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间（普通缓存）
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 2. 将任意java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题（针对热点缓存）
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 设置逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 存入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 解决缓存穿透的逻辑
     * 3. 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题（普通缓存）
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){

        String key = keyPrefix + id;
        //1. 从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if(StrUtil.isNotBlank(json)){
            //3. 缓存命中，直接返回商铺信息
            return JSONUtil.toBean(json, type);
        }
        // 解决缓存穿透，命中空值，说明缓存不存在，返回错误信息
        if(json != null){
            return null;
        }
        //4. 未命中，根据id从数据库中查询
        R r = dbFallback.apply(id);
        //5.1 数据库不存在，返回错误
        if(r == null){
            // 被动解决缓存穿透，不存在，就存入空值
            this.set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }
        //5.2 数据库中的数据存在，将数据写入到redis
        this.set(key, r, time, unit);
        //6. 返回数据信息
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 逻辑过期解决缓存击穿的逻辑
     * 主要解决热点key的问题
     * 4. 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题（针对热点缓存）
     */
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){

        String key = keyPrefix + id;
        //1. 从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if(StrUtil.isBlank(json)){
            //2.1 缓存未命中，直接返回空
            return null;
        }

        //2.2 缓存命中，先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject)redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //3. 判断缓存是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //3.1 未过期，返回信息
            return r;
        }
        //3.2 缓存过期，需要缓存重建
        //4. 缓存重建
        //4.1 尝试获取互斥锁
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        //4.2 判断是否获取成功
        if(isLock){

            // 获取锁成功，再次检测redis缓存是否过期，做 DoubleCheck，如果存在则无需缓存重建
            json = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isBlank(json)){
                return null;
            }
            redisData = JSONUtil.toBean(json, RedisData.class);
            r = JSONUtil.toBean((JSONObject)redisData.getData(), type);
            expireTime = redisData.getExpireTime();
            if(expireTime.isAfter(LocalDateTime.now())){
                return r;
            }

            //4.3 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(LOCK_SHOP_KEY + id);
                }
            });
        }
        //4.4 无论获取成功与否，都返回过期的信息
        return r;
    }

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

}
