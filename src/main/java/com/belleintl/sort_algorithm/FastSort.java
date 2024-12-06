package com.belleintl.sort_algorithm;

/**
 * @ClassName: FastSort
 * @Description: 快速排序
 * @Author: zhipengl01
 * @Date: 2023/11/27
 */
public class FastSort {
    /**
     * 基本思想: 通过一趟排序将待排记录分隔成独立的两部分, 其中一部分记录的关键字比另一部分的关键字小, 则可分别对这两部分记录继续进行排序, 已达到整个序列有序
     * 算法描述: 1.从数列中挑出一个元素, 称为"基准"(pivot)  2.重新排序数列, 所有元素比基准值小的摆放在基准前面, 所有元素比基准值大的摆放在基准后面(相同的数可以放到任意一边), 这个动作叫做分区操作  3.递归地把小于基准值元素的子数列和大于基准值元素的子序列排序
     */
    public static void main(String[] args) {
        int[] arr = new int[]{6, 5, 3, 1, 8, 7, 2, 4};
    }

    public static void sort(int[] arr, int[] left, int[] right) {
        int i = 0, j = 0;
        int pivot = (int) Math.floor(arr.length / 2.0);
        System.out.println("pivot: "+pivot);
        for (int ele : arr) {
            if (ele > pivot) {
                right[i++] = ele;
            } else {
                left[j++] = ele;
            }
        }
    }

}
