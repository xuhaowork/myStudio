package com.self.core.dataTypes;

import com.self.base.BaseMain;

/**
 * java的数据类型
 * ----
 * 1.基本数据类型
 * --1.1 数值类型
 * ----1.1.1 整数 byte short int long --分别是: 1, 2, 4, 8字节
 * ----1.1.2 浮点型 float double --分别是: 4, 8字节
 *
 * --1.2 字符型 char --两个字节 java采用unicode16位定长编码
 * --1.3 布尔型 boolean
 *
 * 2.引用类型
 * --2.1 类
 * --2.2 接口 interface
 * --2.3 数组 array
 */
public class testCompile extends BaseMain {
    public static void main(String[] args) {
        System.out.println("this is a test file.");
        byte b1= 123;
        int i1 = 129;
        long l1 = 122354544633354L;
        double d1 = 21343433232.0001545D;

        int b11 = b1;

        println(b11);



    }

}
