package com.hy.flink.streaming.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Author hy
 * @Date 2022-02-13 16:16:36
 * @Description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Demo implements Serializable {

    private String name;
    private Long time;
}
