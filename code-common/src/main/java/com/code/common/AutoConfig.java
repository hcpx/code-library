package com.code.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/20 11:08
 */
@Slf4j
@Configuration
@ComponentScan(basePackageClasses = AutoConfig.class)
public class AutoConfig {
}
