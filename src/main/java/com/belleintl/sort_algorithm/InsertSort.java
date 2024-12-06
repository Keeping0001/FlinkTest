package com.belleintl.sort_algorithm;

import java.util.Arrays;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * @ClassName: InsertSort
 * @Description: 插入排序: 采用从第二个值开始往前查找排序, 若存在差异, 则将位置后移一位
 * @Author: zhipengl01
 * @Date: 2023/11/10
 */
public class InsertSort {
    public static void main(String[] args) {
        int[] testArray = new int[]{6, 5, 3, 1, 8, 7, 2, 4};
//        for (int i = 1; i < testArray.length; i++) {
//            int tmp = testArray[i];
//            int j = i - 1;
//            while (j >= 0 && testArray[j] > tmp){
//                testArray[j+1] = testArray[j];
//                j--;
//            }
//            testArray[j+1] = tmp;
//        }
        sort(testArray, false);
        System.out.println(Arrays.toString(testArray));
    }

    public static void sort(int[] arr, boolean asc) {
        for (int i = 1; i < arr.length; i++) {
            int tmp = arr[i];
            int j = i - 1;
            while (j >= 0 && ((arr[j] > tmp && asc) || (arr[j] < tmp && !asc))) {
                arr[j+1] = arr[j];
                j--;
            }
            arr[j+1] = tmp;
        }
    }
}
