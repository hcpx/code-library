package com.hy.springboot.kafka.streams.domain;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author HY
 * @version 0.0.1
 * @date 2021/2/1 10:35
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageEvent implements Serializable {

    private String id;
    private String name;
}
