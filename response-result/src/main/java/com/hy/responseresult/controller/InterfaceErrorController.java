package com.hy.responseresult.controller;

import com.hy.responseresult.response.ResponseResult;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.ServletWebRequest;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * @Author hy
 * @Date 2020/11/20 21:21:49
 * @Description
 */
@RestController
@RequiredArgsConstructor
public class InterfaceErrorController implements ErrorController {
    private final ErrorAttributes errorAttributes;

    @RequestMapping(value = "/error")
    public ResponseResult<Object> myError(HttpServletRequest request) {
        ServletWebRequest requestAttributes = new ServletWebRequest(request);
        Map<String, Object> attr = this.errorAttributes.getErrorAttributes(requestAttributes, ErrorAttributeOptions.defaults());
        return ResponseResult.failure((int) attr.get("status"), attr.get("error"));
    }

    @Override
    public String getErrorPath() {
        return "/error";
    }
}
