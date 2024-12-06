package com.belleintl.finkbean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @ClassName: Mail
 * @Description:
 * @Author: zhipengl01
 * @Date: 2022/6/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Mail {
    private String appKey;
    private String appVersion;
    private String deviceId;
    private String phoneNo;

    @Override
    public String toString() {
        return "Mail{" +
                "appKey='" + appKey + '\'' +
                ", appVersion='" + appVersion + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", phoneNo='" + phoneNo + '\'' +
                '}';
    }
}
