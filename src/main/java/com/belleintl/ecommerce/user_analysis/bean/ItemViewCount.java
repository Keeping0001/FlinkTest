package com.belleintl.ecommerce.user_analysis.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @ClassName: Item
 * @Description: 商品访问情况POJO
 * @Author: zhipengl01
 * @Date: 2021/12/30
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
