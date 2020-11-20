package com.hy.responseresult.controller;

import com.hy.responseresult.eneity.People;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Api(tags = "测试")
@RestController
@RequestMapping("/myPath")
public class Controller {

    @ApiOperation("sss")
    @GetMapping("/sayHello")
    public Map<String, String> syaHello() {
        Map<String, String> map = new HashMap<>();
        map.put("1", "a");
        return map;
    }

    @GetMapping("/say")
    public People say() {
        String str = "xiaoming";
        return new People(str, 1);
    }

    @RequestMapping("/sayString")
    public String sayString() {
        return "Hello world!";
    }

    @GetMapping("/sayList")
    public List<String> sayList() {
        ArrayList<String> list = new ArrayList<>();
        String str = "Hello world!";
        list.add(str);
        return list;
    }


}
