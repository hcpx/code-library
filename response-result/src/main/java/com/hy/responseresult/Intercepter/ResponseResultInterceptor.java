package com.hy.responseresult.Intercepter;

import com.hy.responseresult.annotation.JsonResult;
import com.hy.responseresult.config.WebConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Method;


@Component
@Slf4j
public class ResponseResultInterceptor extends HandlerInterceptorAdapter {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        boolean result = false;
        if (handler instanceof HandlerMethod) {
            final HandlerMethod handlerMethod = (HandlerMethod) handler;
            final Class<?> beanType = handlerMethod.getBeanType();
            final Method method = handlerMethod.getMethod();
            if (beanType.isAnnotationPresent(JsonResult.class)) {
                request.setAttribute(WebConstant.RESPONSE_RESULT_JSON, beanType.getAnnotation(JsonResult.class));
                result = true;
            } else if (method.isAnnotationPresent(JsonResult.class)) {
                request.setAttribute(WebConstant.RESPONSE_RESULT_JSON, method.getAnnotation(JsonResult.class));
                result = true;
            }
        }
        return result;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) {
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
    }
}
