package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author hy
 * @date 2022/1/13 9:51 下午
 * @description
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class YearMovie {

    private String title;
    private Integer releaseYear;
    private Long totalSales;
}
