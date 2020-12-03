package com.hy.security.springboot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hy.security.springboot.dao.UserDao;
import com.hy.security.springboot.model.UserDto;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Administrator
 * @version 1.0
 **/
@Service
@RequiredArgsConstructor
public class SpringDataUserDetailsService implements UserDetailsService {

    private final UserDao userDao;

    private final ObjectMapper objectMapper;


    //根据 账号查询用户信息
    @Override
    @SneakyThrows
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        //将来连接数据库根据账号查询用户信息
        UserDto userDto = userDao.getUserByUsername(username);
        if (userDto == null) {
            //如果用户查不到，返回null，由provider来抛出异常
            throw  new UsernameNotFoundException("");
        }
        //根据用户的id查询用户的权限
        List<String> permissions = userDao.findPermissionsByUserId(userDto.getId());
        //将permissions转成数组
        String[] permissionArray = new String[permissions.size()];
        permissions.toArray(permissionArray);
        return User.withUsername(objectMapper.writeValueAsString(userDto)).password(userDto.getPassword()).authorities(permissionArray).build();
    }
}
