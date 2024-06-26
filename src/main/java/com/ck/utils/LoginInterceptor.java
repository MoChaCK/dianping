package com.ck.utils;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 对前一个拦截器，进行真正的拦截
 */
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 判断是否需要拦截（ThreadLocal中是否有用户）
        if(UserHolder.getUser() == null){
            //没有用户，需要拦截，设置状态码
            response.setStatus(401);
            return false;
        }
        return true;
    }

}
