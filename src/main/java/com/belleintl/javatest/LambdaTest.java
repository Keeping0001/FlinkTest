package com.belleintl.javatest;

import java.util.HashMap;
import java.util.Locale;

/**
 * @ClassName: LambdaTest
 * @Description: Lambda测试
 * @Author: zhipengl01
 * @Date: 2024/3/11
 */

interface Addable {
    int add(int a, int b);
}

public class LambdaTest {
    public static void main(String[] args) {
        useAddable((int x, int y) -> {
            return x + y;
        });

        HashMap<Integer, String> map = new HashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        map.replaceAll((k, v)->v.toUpperCase(Locale.ROOT));

        System.out.println(map);

    }

    public static void useAddable(Addable a) {
        int sum = a.add(10, 20);
        System.out.println(sum);
    }
}
