package com.ck.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ck.dto.LoginFormDTO;
import com.ck.dto.Result;
import com.ck.dto.UserDTO;
import com.ck.entity.User;
import com.ck.mapper.UserMapper;
import com.ck.service.IUserService;
import com.ck.utils.RedisConstants;
import com.ck.utils.RegexUtils;
import com.ck.utils.SystemConstants;
import com.ck.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ck.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone) {
        //1. 校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //2. 不符合就返回错误信息
            return Result.fail("手机号格式错误");
        }
        //3. 符合，生成校验码
        String code = RandomUtil.randomNumbers(6);
        //4. 保存验证码到redis，用手机号作为键，验证码code作为值 // set key value ex 120
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5. 发送验证码
        log.debug("发送短信验证码成功，验证码：{}", code);
        //6. 返回
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        //1. 验证手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误");
        }
        //2. 校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone); //redis中的code
        String code = loginForm.getCode(); //页面表单中提交的code
        if(cacheCode == null || !cacheCode.equals(code)){
            //3.不一致则报错
            return Result.fail("验证码错误");
        }
        //4. 一致则根据手机号查询用户
        User user = query().eq("phone", phone).one();
        //5. 判断用户是否存在
        if(user == null){
            //6. 不存在创建并保存新用户
            user = createUserWithPhone(phone);
        }
        //7. 保存用户信息到redis中
        //7.1 随机生成token，作为登录令牌，并作为键存储
        String token = UUID.randomUUID().toString(true);
        String tokenKey = LOGIN_USER_KEY + token;
        //7.2 将User对象转为hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //存储时将User对象的所有属性转为String类型存储到hash中
        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        //7.3 存储
        stringRedisTemplate.opsForHash().putAll(tokenKey, map);
        //7.4 设置存储的有效期
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
        //8. 返回token
        return  Result.ok(token);
    }

    @Override
    public Result queryUserById(Long userId) {
        // 查询用户信息
        User user = getById(userId);
        if(user == null){
            return Result.ok();
        }
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 将用户信息返回
        return Result.ok(userDTO);
    }

    @Override
    public Result sign() {
        //1.获取当前登录的用户
        Long userId = UserHolder.getUser().getId();
        //2.获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1. 获取本月截止今天为止的所有的签到记录
        //1.1 获取当前登录的用户
        Long userId = UserHolder.getUser().getId();
        //1.2 获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //1.3 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //1.4 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //2. 获取本月截止今天为止的所有的签到记录，返回的是一个十进制的数字，BITFIELD sign:1010:202403 GET u7 0
        List<Long> list = stringRedisTemplate.opsForValue()
                .bitField(key,
                        BitFieldSubCommands.create()
                                .get(BitFieldSubCommands.BitFieldType.signed(dayOfMonth)).valueAt(0));
        if(list == null || list.isEmpty()){
            // 没有任何签到结果
            return Result.ok(0);
        }
        Long num = list.get(0);
        if(num == null){
            return Result.ok(0);
        }
        //3.遍历循环
        int count = 0;
        //3.1 数字与1做位运算，得到最后一个数字的bit位 判断该bit位是否为0
        while(true) {
            if((num & 1) == 0){
                //3.2 为0，说明未签到，返回
                break;
            } else {
                //3.3 不为0，计数器加1
                count++;
            }
            //3.4 将数字右移1位
            num >>>= 1;
        }

        return Result.ok(count);

    }

    private User createUserWithPhone(String phone) {
        //1. 创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //2. 保存用户
        save(user);
        return user;
    }
}
