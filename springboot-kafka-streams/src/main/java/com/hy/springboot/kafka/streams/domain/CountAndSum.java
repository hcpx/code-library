package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author hy
 * @date 2022/1/15 11:06 下午
 * @description
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CountAndSum {
    private Integer count;
    private Double sum;
}
