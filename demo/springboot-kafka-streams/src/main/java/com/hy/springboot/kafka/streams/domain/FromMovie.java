package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hy
 * @date 2022/1/9 9:52 上午
 * @description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FromMovie {

    private Integer id;
    private String title;
    private String genre;

}
