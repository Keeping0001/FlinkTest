package com.belleintl.ecommerce.user_analysis.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @ClassName: UserBehavior
 * @Description: 用户行为POJO
 * @Author: zhipengl01
 * @Date: 2021/12/30
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
