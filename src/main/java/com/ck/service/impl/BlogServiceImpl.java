package com.ck.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ck.dto.Result;
import com.ck.dto.UserDTO;
import com.ck.entity.Blog;
import com.ck.entity.User;
import com.ck.mapper.BlogMapper;
import com.ck.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ck.service.IUserService;
import com.ck.utils.SystemConstants;
import com.ck.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ck.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 根据 id 查询博客信息
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        //1. 查询Blog
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("笔记不存在！");
        }
        //2. 查询blog有关的用户
        queryBlogUser(blog);

        //3. 查询用户是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    /**
     * 分页查询博客
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });

        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    private void isBlogLiked(Blog blog) {
        // 1. 获取登录用户
        UserDTO user = UserHolder.getUser();
        if(user == null){
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2. 查询在redis中Blog的id及对应的用户id，判断当前用户是否点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }



    /**
     * 点赞功能
     * @return
     */
    @Override
    public Result likeBlog(Long id) {

        // 1. 获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 2. 查询在redis中Blog的id及对应的用户id，判断当前用户是否点赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if(score == null){
            // 3. 如果未点赞，可以点赞
            // 3.1 修改数据库liked + 1   update tb_blog set liked = liked + 1 where id = ?
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if(isSuccess) {
                // 3.2 将当前用户id存入redis的set集合中
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 4. 如果点过赞，取消点赞
            // 4.1 修改数据库liked - 1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if(isSuccess) {
                // 4.2 将当前用户从redis的set集合中移除
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    /**
     * 查询博客的top5点赞用户
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLikes(Long id) {
        // 1. 查询top5点赞用户
        String key = BLOG_LIKED_KEY + id;
        Set<String> top5Set = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5Set == null || top5Set.isEmpty()){
            return Result.ok();
        }
        // 2. 解析其中的用户id
        List<Long> ids = top5Set.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 3. 根据用户id查询用户 WHERE id in (5, 1) ORDER BY FIELD(id, 5, 1)
        List<UserDTO> userDTOs = userService.query().in("id", ids).last("order by field (id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 4. 返回
        return Result.ok(userDTOs);
    }

    @Override
    public Result queryBlogByUserId(Integer current, Long id) {
        // 根据用户查询
        Page<Blog> page = query().eq("user_id", id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }
}
