package com.hy.springboot.kafka.streams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author hy
 * @date 2022/1/18 8:23 下午
 * @description
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SelectKeyInput {

    private Integer id;
    private String firstName;
    private String lastName;
    private String phoneNumber;
}
