package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hy
 * @date 2022/1/13 9:54 下午
 * @description
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class YearlyMovieFigures {

    private Long minTotalSales;
    private Integer releaseYear;
    private Long maxTotalSales;

}
