package com.hy.responseresult.controller;

import com.hy.responseresult.annotation.JsonResult;
import com.hy.responseresult.eneity.People;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/myPath")
public class Controller {

    @JsonResult
    @GetMapping("/sayHello")
    public Map<String, String> syaHello() {
        Map<String, String> map = new HashMap<>();
        map.put("1", "a");
        return map;
    }

    @JsonResult
    @GetMapping("/say")
    public People say() {
        String str = "xiaoming";
        return new People(str, 1);
    }

    @JsonResult
    @RequestMapping("/sayString")
    public String sayString() {
        return "Hello world!";
    }

    @JsonResult
    @GetMapping("/sayList")
    public List<String> sayList() {
        ArrayList<String> list = new ArrayList<>();
        String str = "Hello world!";
        list.add(str);
        return list;
    }


}
