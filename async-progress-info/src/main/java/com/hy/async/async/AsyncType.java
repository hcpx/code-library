package com.hy.async.async;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public enum AsyncType {
    normal("normal", "普通");

    private String type;
    private String name;

    AsyncType(String type, String name) {
        this.type = type;
        this.name = name;
    }
}
