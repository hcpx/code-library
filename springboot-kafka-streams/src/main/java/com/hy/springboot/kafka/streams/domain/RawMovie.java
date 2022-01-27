package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author hy
 * @date 2022/1/16 11:20 上午
 * @description
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RawMovie {

    private Integer id;
    private String title;
    private String genre;
}
