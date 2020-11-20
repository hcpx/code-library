package com.hy.responseresult.advice;

import com.hy.responseresult.exception.APIException;
import com.hy.responseresult.response.ResponseResult;
import com.hy.responseresult.response.ResultCode;
import jdk.nashorn.internal.runtime.logging.Logger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.NoHandlerFoundException;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/20 16:52
 */
@RestControllerAdvice
@Slf4j
public class ExceptionControllerAdvice {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseResult MethodArgumentNotValidExceptionHandler(MethodArgumentNotValidException e) {
        // 从异常对象中拿到ObjectError对象
        ObjectError objectError = e.getBindingResult().getAllErrors().get(0);
        // 然后提取错误提示信息进行返回
        return ResponseResult.failure(ResultCode.PARAM_IS_INVALID, objectError.getDefaultMessage());
    }

    @ExceptionHandler(APIException.class)
    public ResponseResult APIExceptionHandler(APIException apiException) {
        return ResponseResult.failure(apiException);
    }

    @ExceptionHandler(Throwable.class)
    public ResponseResult handlerException(Exception e) {
        if (e instanceof NoHandlerFoundException) {
            return ResponseResult.failure(ResultCode.NOT_FOUND);
        }else {
            log.error("统一异常处理:",e);
            return ResponseResult.failure(ResultCode.BACKGROUND_ERROR,e.getMessage());
        }
    }
}

