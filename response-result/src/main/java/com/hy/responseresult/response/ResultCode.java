package com.hy.responseresult.response;

import lombok.Getter;

@Getter
public enum ResultCode {

    /* 成功状态码*/
    SUCCESS(200,"成功"),

    /*参数错误 */
    PARAM_IS_INVALID(1001,"参数无效"),

    /*参数错误 */
    NOT_FOUND(404,"不存在的页面"),

    /*参数错误 */
    BACKGROUND_ERROR(500,"后台未知错误,请联系管理员"),

    /*参数错误 */
    STRING_BODY_IS_INVALID(1002,"返回String类型转换错误"),

    /* 用户错误*/
    USER_NOT_LOGGED_ID(2001,"用户未登录，访问的路径需要验证，请登录");

    private Integer code;
    private String message;

    ResultCode(Integer code, String message) {
        this.code = code;
        this.message = message;
    }
}
