package com.hy.async.async;

import lombok.Data;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/18 14:18
 */
@Data
public class ProgressModel {
    private volatile boolean isFinish;
    private int total;
    private int finished;
}
