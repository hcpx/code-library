package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hy
 * @date 2022/1/14 12:13 下午
 * @description
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Rating {

    private String title;
    private Integer releaseYear;
    private Double rating;
    private String timestamp;
}
