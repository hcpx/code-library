package com.hy.responseresult.service;

import com.hy.responseresult.eneity.User;
import org.springframework.stereotype.Service;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/20 18:04
 */
@Service
public class UserService {
    public String addUser(User user) {
        // 直接编写业务逻辑
        return "success";
    }
}
