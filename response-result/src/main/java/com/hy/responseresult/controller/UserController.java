package com.hy.responseresult.controller;

import com.hy.responseresult.eneity.User;
import com.hy.responseresult.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/20 18:03
 */
@RestController
@RequestMapping("user")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;


    //    {
//        "account": "12345678",
//            "email": "123@qq.com",
//            "id": 0,
//            "password": "123"
//    }
    @PostMapping("/addUser")
    public String addUser(@RequestBody @Valid User user) {
        return userService.addUser(user);
    }

    @GetMapping("/getUser")
    public User getUser() {
        User user = new User();
        user.setId(1L);
        user.setAccount("12345678");
        user.setPassword("12345678");
        user.setEmail("123@qq.com");
        return user;
    }
}
