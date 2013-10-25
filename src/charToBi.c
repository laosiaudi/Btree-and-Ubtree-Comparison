#include<stdio.h>
#include<string.h>
//数字转二进制
void int2bin(int num, char* bin4) //bin4 为4个元素的字符串
{
	char atoi[2] = "01";
    int	i = 3;
	int yu;
	//转二进制
	while (num > 0)
	{
		yu = num % 2;
		bin4[i] = atoi[yu];
		--i;
		num = (num - yu) / 2;
	}
	//前面补0
	for (; i >= 0; --i)
		bin4[i] = '0';
}

// 单个字符串转二进制
void str2bin(char* s, char* bin) 
{
	//对s中每个字符进行char->int
	int size = strlen(s);
	int arr[size];
	int i, j;
	if (size == 10)
	{
		for (i = 0; i < 4; ++i)
			arr[i] = s[i] - 48;
		arr[i] = (s[5] - 48) * 10 + (s[6] - 48);
		++i;
		for (j = 8; j < 10; ++j, ++i)
			arr[i] = s[j] -  48;
	}
	else
		for (j = 2, i = 0; j < 4; ++j, ++i)
			arr[i] = s[j] - 48;

	size = i;
	for (i = 0; i < size; ++i)
	{
		char four[4] = "\0";
	   	int2bin(arr[i], four);
		strncat(bin, four, 4); //把转成的二进制串four复制到bin后面
	}
}

void multi_str2bin(char* whole, char* str1, char* str2) 
{
	//bin1 bin2 储存每个字符串转换后的二进制串
	char bin1[28] = "\0";
	char bin2[8] = "\0";
	str2bin(str1, bin1);
	str2bin(str2, bin2);
	int len1 = strlen(bin1);
	int len2 = strlen(bin2);
	//合并
	int p = 0, q = 0, j = 0;
	while ((p < len1) || (q < len2))
	{
		if(p < len1)
		{
			whole[j] = bin1[p];
			++j;
		}
		if(q < len2)
		{
			whole[j] = bin2[q];
			++j;
		}
		++p; ++q;
	}
       whole[36] = '\0';
}



