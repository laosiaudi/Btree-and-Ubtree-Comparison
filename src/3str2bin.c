#include<stdio.h>
#include<string.h>

// author: yakiang

//数字转二进制
void int2bin_(int num, char* bin4, int len) 
{
	char atoi[2] = "01";
    int	i = len - 1;
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
void str2bin_(char* s, char* bin) 
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
		arr[i] = (s[8] - 48) * 10 + (s[9] - 48);
		size = i;
		for (i = 0; i < size; ++i)
		{
			char four[4] = "\0";
	   		int2bin_(arr[i], four, 4);
			strncat(bin, four, 4); 
		}
		char five[5] = "\0";
		int2bin_(arr[size], five, 5);
		strncat(bin, five, 5);
	}
	else if (s[1] == '.')
	{
		for (j = 2, i = 0; j < 4; ++j, ++i)
			arr[i] = s[j] - 48;
		size = i;
		for (i = 0; i < size; ++i)
		{
			char four[4] = "\0";
	   		int2bin_(arr[i], four, 4);
			strncat(bin, four, 4); 
		}
	}
	else
	{
		for (j = 0, i = 0; j < 2; ++j, ++i)
			arr[i] = s[j] - 48;
		size = 2;
		for (i = 0; i < size; ++i)
		{
			char four[4] = "\0";
	   		int2bin_(arr[i], four, 4);
			strncat(bin, four, 4); 
		}
	}
}

void multi_str2bin_(char* whole, char* str1, char* str2, int third)
{
	char num2char[10] = "0123456789";
	char bin1[25] = "\0";
	char bin2[8] = "\0";
	char bin3[8] = "\0";
	char str3[2];
	int i, tm;
	for (i = 1; i >= 0; --i)
	{
		tm = third % 10;
		str3[i] = num2char[tm];
		third = (third - tm) / 10;
	}

	str2bin_(str1, bin1);
	str2bin_(str2, bin2);
	str2bin_(str3, bin3);
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
			whole[j] = bin3[q];
			++j;
		}
		++p; ++q;
	}
	whole[41] = '\0';
}



