package com.ck.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.ck.entity.ShopType;
import com.ck.mapper.ShopTypeMapper;
import com.ck.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ck.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 给店铺类型添加缓存
     * @return
     */
    @Override
    public List<ShopType> queryAll() {

        //1. 根据redis查询店铺类型
        String shopTypeJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOPTYPE_KEY);
        //2. 存在，直接返回店铺类型的列表
        if(StrUtil.isNotBlank(shopTypeJson)){
           return JSONUtil.parseArray(shopTypeJson).toList(ShopType.class);
        }
        //3. 不存在，到数据库中查询
        List<ShopType> shopTypeList = list();
        //4. 数据库中不存在，返回404错误
        if(shopTypeList == null){
            return null;
        }
        //5. 数据库中存在
        //6. 将店铺类型写入到redis作为缓存
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOPTYPE_KEY, String.valueOf(JSONUtil.parse(shopTypeList)));
        //7. 返回店铺类型列表
        return shopTypeList;
    }
}
