package com.hy.responseresult.response;

import com.hy.responseresult.exception.APIException;
import lombok.Data;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.Serializable;

@ResponseBody
@Data
public class ResponseResult<T> implements Serializable {

    private Integer code;

    private String message;

    private T data;

    public ResponseResult() {
    }

    public ResponseResult(Integer code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static ResponseResult success() {

        ResponseResult result = new ResponseResult();
        result.setCode(ResultCode.SUCCESS.getCode());
        result.setMessage(ResultCode.SUCCESS.getMessage());
        return result;
    }

    public static <T> ResponseResult success(T data) {
        ResponseResult result = new ResponseResult();
        result.setCode(ResultCode.SUCCESS.getCode());
        result.setMessage(ResultCode.SUCCESS.getMessage());
        result.setData(data);
        return result;
    }

    public static ResponseResult failure(ResultCode resultCode) {
        ResponseResult result = new ResponseResult();
        result.setCode(resultCode.getCode());
        result.setMessage(resultCode.getMessage());
        return result;
    }

    public static <T> ResponseResult failure(ResultCode resultCode, T data) {
        ResponseResult result = new ResponseResult();
        result.setCode(resultCode.getCode());
        result.setMessage(resultCode.getMessage());
        result.setData(data);
        return result;
    }

    public static ResponseResult failure(APIException apiException) {
        ResponseResult result = new ResponseResult();
        result.setCode(apiException.getResultCode().getCode());
        result.setMessage(apiException.getMessage());
        return result;
    }

}
