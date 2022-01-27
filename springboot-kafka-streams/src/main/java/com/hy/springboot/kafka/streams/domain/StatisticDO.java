package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author HY
 * @version 0.0.1
 * @date 2021/2/1 16:09
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class StatisticDO {

    private String name;
    private Integer count;

    public static StatisticDO reduce(StatisticDO statisticDO1, StatisticDO statisticDO2) {
        return new StatisticDO(statisticDO1.getName(), statisticDO1.count + statisticDO2.count);
    }

    public StatisticDO(Integer count) {
        this.count = count;
    }
}
