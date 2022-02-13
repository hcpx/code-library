package com.hy.springboot.kafka.streams.domain;

import java.util.List;
import java.util.Set;
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
public class MessageEventArr {

    private String id;
    private List<String> name;
}
