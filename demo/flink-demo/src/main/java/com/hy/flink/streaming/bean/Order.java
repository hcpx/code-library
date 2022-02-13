package com.hy.flink.streaming.bean;

import java.io.Serializable;
import lombok.Data;

/**
 * @author hy
 * @date 2022/2/8 4:32 下午
 * @description
 */
@Data
public class Order implements Serializable {

    private String id;
    private Double price;
    private String orderType;
    private Long timestamp;
}
