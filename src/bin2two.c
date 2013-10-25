#include <stdio.h>
#include <string.h>
void bin2two(char* whole, char* bin1, char* bin2)
{
	int i;
	for (i = 0; i < 16; ++i)
	{
		if (i % 2 == 0)
			bin1[i/2] = whole[i];
		else
			bin2[i/2] = whole[i];
	}
	int j = 8;
	for (; i < 36; ++i, ++j)
		bin1[j] = whole[i];
}
void bin2three(char* whole, char* bin1, char* bin2, char* bin3)
{
	int i;
	for (i = 0; i < 24; ++i)
	{
		if ((i + 1) % 3 == 0)
			bin3[i/3] = whole[i];
		else if((i + 1) % 3 == 1)
			bin1[i/3] = whole[i];
		else
			bin2[i/3] = whole[i];
	}
	int j = 8;
	for (; i < 40; ++i, ++j)
		bin1[j] = whole[i];
}

typedef struct{
	int dimension;
	int index;
}position;

position where(int entry)
{
	position pos;
	if (entry < 16)
	{
		if ((entry % 2 != 0) && (entry < 16))
			pos.dimension = 2;
		else
			pos.dimension = 1;
		pos.index = entry / 2;
	}
	else
	{
		pos.dimension = 1;
		pos.index = entry - 8;
	}
	return pos;
}

position where_in3(int entry)
{
	position pos;
	if (entry < 24)
	{
		pos.dimension = (entry + 1) % 3;
		if (pos.dimension == 0)
			pos.dimension = 3;
		pos.index = entry / 3;
	}
	else
	{
		pos.dimension = 1;
		pos.index = entry - 16;
	}
	return pos;
}


int find_pos(position pos)
{
	int index = pos.index;
	int dim = pos.dimension;
	if (index < 8)
		return (index * 2 + dim - 1);
	else
		return (index + 8);
}

int find_pos_in3(position pos)
{
	int index = pos.index;
	int dim = pos.dimension;
	if (index < 8)
		return (index * 3 + dim - 1);
	else
		return (index + 16);
}

/*int main()
{
	char whole[37] = "\0";
	char bin1[29] = "\0";
	char bin2[9] = "\0";
	//scanf("%s", whole);
	//bin2two(whole, bin1, bin2); //convert to two bins
	//printf("%s\n", bin1);
	//printf("%s\n", bin2);
	
	//position p = where(n); // return the position struct
	position p ;
	int a, b;
	while (scanf("%d %d", &a, &b))
	{
		p.dimension = a;
		p.index = b;
		printf("%d\n", find_pos(p));
	}
	return 0;
}*/


