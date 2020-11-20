package com.hy.responseresult.exception;

import com.hy.responseresult.response.ResultCode;
import lombok.Getter;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/20 16:56
 */
@Getter
public class APIException extends RuntimeException {
    private ResultCode resultCode;
    private String msg;

    public APIException(ResultCode resultCode, String msg) {
        super(msg);
        this.resultCode = resultCode;
        this.msg = msg;
    }
}
