// cpp.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include "pch.h"
#include <iostream>


int main()
{
	int a = 0x1654F;
	// 赋值操作时深拷贝
	int b = a;
	a = a + b;
	int arr[] = { 1, 2, 3, 5 };
	char crr[5][3] = {
		{91, 92, 93},
	{0},
	{31,32, 33},
	{11, 12, 13},
	{2, 3, 5}
	};

	// 内存用于存储数据, 最小单元为8bit, 每个地址都有编号
	// 每个变量都有大小, 是指变量在内存中所占的地址, 地址的编号称为指针, 变量名和指针其实就是人名和门牌号的关系
	// 每个变量的内存地址都有特定的变量
	// 变量索引就是从内存中读取值
	// 变量赋值就是将内存中写入数据
	// 每次程序运行变量地址是由程序分配的
	printf("size = %d \n", sizeof(a));
	printf("position = %p \n", &a);

	// int* 表示原生类型int的地址, 我们称之为指针类型
	int* pa = &a; // 此外还有其他方式 int * pa = &a; int *pa = &a;等表示方式
	printf("value of a, %d \n", a);
	printf("position of a, pa = %p \n", pa);

	// 利用指针赋值
	*pa = 22; 
	// 此时*表示一个函数, 即索引pa内存中的值, 由于pa为a在内存中分配的地址, 改动*pa也是改动a.
	// 另外虽然pa值为int, 但pa的值不能认为是int类型, 它实际的值为int*, 因此*12是索引不到的
	printf("value of a, %d \n", a);
	printf("position of a, , after asign poiner, pa = %p \n", &a); 
	*pa += 2;
	printf("value of a, %d \n", a);
	// 总结: 效果与直接赋值相同
	

	// 数组
	int someArr[4] = { 1, 2, 3, 4 };
	printf("position of someArr, %p \n", &someArr);
	printf("position of first element of someArr, %p \n", &someArr[0]);
	someArr[0] = 100;
	printf("position of someArr, %p \n", &someArr);
	printf("position of first element of someArr, %p \n", &someArr[0]);
	int* parr = someArr;
	parr += 1;
	printf("the position of parr, %p \n", parr);
	printf("the first elemnt of parr, %d \n", *parr);
	printf("the position of someArr, %p \n", someArr);
	printf("the first element of someArr, %d \n", someArr[0]);
	printf("parr: %p \n", parr);
	parr += 1;
	printf("parr: %p \n", parr);
	*parr = 10000;
	printf("the third element of someArr, %d \n", someArr[2]);
	printf("the first and only element of someArr, %d \n", pa[0]);

	double d11 = 100.0;
	double* pdd = &d11;
	printf("value: %d \n", pdd);
	pdd += 1;
	printf("value: %d \n", pdd);
	pdd;
	// 总结: 
	// 数组名本身就是指针, 不用&符号
	// 数组的指针就是第一个元素的指针
	// 指针本身的+1表示指针移到下一个内存单元中(int类指针索引其实是加了4, double类是加了8，一次类推)
	// 此时数组的首个元素就变成了原来的第二个元素, 但注意原数组(原指针)指向的位置不变
	// 数组, 本身是一种内存指针

	// 案例: 如何遍历一个数组
	for (int* p = someArr; p < someArr + 4; p++) {
		printf("someArr: p: %p, value: %d \n", p, *p); 
	}

	// 数组越界 此时在工程中容易引发非常难以排查的bug
	printf("value: %d \n", *(someArr + 4));
	*(someArr + 4) = -100;

	int uu = 1;
	switch (uu) {
	case 1:
		printf("one \n");
		break; // break需要添加，switch相当于自动帮你进行跳转（有点类似于goto）需要你自动break
	case 2:
		printf("two \n");
	case 3:
		printf("three \n");
	default: // default表示没有符合上述的switch情况时进行的操作, 也可以不带
		printf("default");
	}
	// 注意： 
	// 1)switch必须是整数而不能是浮点数
	// 2)必须是常量
	// 3)switch能被if..else..完全覆盖


	//char c = 'a';
	//char cuu[] = "abc";
	
	//printf("this is a test file %s \n", cuu);

	// 局部变量声明
	//int a = 10;
	//goto inhalve;

 /*inhalve:
	a = 9;
	goto add;
newt:
	a = 11;
	if(a >0)
		add: 
			a = 12;
			return a;
	goto inhalve;

	printf("a is");
	cout << "a 的值：" << a << endl;*/
}

// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单

// 入门提示: 
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5. 转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件
