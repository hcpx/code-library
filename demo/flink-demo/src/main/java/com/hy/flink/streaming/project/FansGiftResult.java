package com.hy.flink.streaming.project;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hy
 * @date 2022/2/18 8:49 下午
 * @description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FansGiftResult {
    //主播ID
    private String hostId;
    //粉丝ID
    private String fansId;
    //礼物数量
    private long giftCount;
    //送礼物时间。时间格式 yyyy-MM-DD HH:mm:SS
    private long windowsEnd;
    //送礼物时间。时间格式 yyyy-MM-DD HH:mm:SS
    private String windowsEndStr;
}
