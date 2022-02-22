package com.hy.flink.streaming.project;

import cn.hutool.core.date.LocalDateTimeUtil;
import java.io.Serializable;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hy
 * @date 2022/2/18 7:41 下午
 * @description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GiftRecord implements Serializable {

    //主播ID
    private String hostId;
    //粉丝ID
    private String fansId;
    //礼物数量
    private long giftCount;
    //送礼物时间。时间格式 yyyy-MM-DD HH:mm:SS
    private LocalDateTime giftTime;

    public GiftRecord(String hostId, String fansId, long giftCount, String giftTime) {
        this.hostId = hostId;
        this.fansId = fansId;
        this.giftCount = giftCount;
        this.giftTime = LocalDateTimeUtil.parse(giftTime);
    }
}
