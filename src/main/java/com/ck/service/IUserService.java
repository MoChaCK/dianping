package com.ck.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.ck.dto.LoginFormDTO;
import com.ck.dto.Result;
import com.ck.entity.User;

import javax.servlet.http.HttpSession;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IUserService extends IService<User> {
    Result sendCode(String phone);

    Result login(LoginFormDTO loginForm, HttpSession session);

    Result queryUserById(Long userId);

    Result sign();

    Result signCount();
}
