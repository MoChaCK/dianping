package com.ck.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ck.dto.Result;
import com.ck.dto.UserDTO;
import com.ck.entity.Follow;
import com.ck.mapper.FollowMapper;
import com.ck.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ck.service.IUserService;
import com.ck.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    /**
     * 关注
     * @param followUserId
     * @param isFollow
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //0. 获取当前登录用户
        Long userId = UserHolder.getUser().getId();

        String key = "follow:" + userId;

        //1. 判断是否关注
        if(isFollow){
            //2. 关注
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean success = save(follow);
            if(success){
                // 把关注的用户的id, 放入redis的set集合中，sadd userId followUserId
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            //3. 取消关注
            boolean success = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", followUserId));
            if(success){
                // 把关注的用户id从redis中的set集合中移除
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        //4. 返回结果
        return Result.ok();
    }

    /**
     * 查询是否关注
     * @param followUserId
     * @return
     */
    @Override
    public Result isFollow(Long followUserId) {
        //1. 获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2. 查询是否关注 select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        //3. 判断并返回
        return Result.ok(count > 0);
    }

    /**
     * 查询共同关注
     * @param id
     * @return
     */
    @Override
    public Result followCommons(Long id) {
        // 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        String key1 = "follow:" + userId;
        // 求交集
        String key2 = "follow:" + id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if(intersect == null || intersect.isEmpty()){
            return Result.ok();
        }
        // 解析集合id
        List<Long> idList = intersect
                .stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        // 查询用户
        List<UserDTO> users = userService.listByIds(idList)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}
