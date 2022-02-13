package com.hy.flink.streaming.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author hy
 * @date 2022/2/8 4:32 下午
 * @description
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order implements Serializable {

    private String id;
    private Double price;
    private String orderType;
    private Long timestamp;
}
