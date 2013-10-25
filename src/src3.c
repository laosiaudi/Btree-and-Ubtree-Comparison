#include <sys/param.h>
#include <sys/stat.h>

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <db.h>
//#include "/home/laosi/DB/db.1.86/btree/btree.h"
#include "btree.h"
#include "charToBi.c"
#include "bin2two.c"
#include "3str2bin.c"

enum S { COMMAND, COMPARE, GET, PUT, REMOVE, SEQ, SEQFLAG, KEY, DATA };

void	 compare __P((DBT *, DBT *));
DBTYPE	 dbtype __P((char *));
void	 dump __P((DB *, int));
void	 err __P((const char *, ...));
void	 get __P((DB *, DBT *));
void	 getdata __P((DB *, DBT *, DBT *));
void	 put __P((DB *, DBT *, DBT *));
void	 rem __P((DB *, DBT *));
char	*sflags __P((int));
void	 synk __P((DB *));
void	*rfile __P((char *, size_t *));
void	 seq __P((DB *, DBT *));
u_int	 setflags __P((char *));
void	*setinfo __P((DBTYPE, char *));
void	 usage __P((void));
void	*xmalloc __P((char *, size_t));

static int __btr_seqset(BTREE *t, EPG *ep, DBT *key, int flags);

static int __btr_first(BTREE *t, const DBT *key, EPG *erval, int *exactp);
static int __btr_seqadv(BTREE *t, EPG *ep, int flags);
int page__seq(const DB *dbp, DBT *key, DBT *data, u_int flags,EPG *e);
void information(int flag,int *savemin,int *savemax, int *outstep,char *dismen,char *up,char *low);
DBTYPE type;				/* Database type. */
void *infop;				/* Iflags. */
u_long lineno;				/* Current line in test script. */
u_int flags;				/* Current DB flags. */
int ofd = STDOUT_FILENO;		/* Standard output fd. */
#define maxxx  99999;
int
main()
	
{
DB *XXdbp;				/* Global for gdb. */
int XXlineno;				/* Fast breakpoint for gdb. */
 
    DB *dbp=(DB*)malloc(sizeof(DB*));
    DBT data, key, keydata;
    char buf[8*1024];
   char *fname = "ind";
	//char *fname = "test1";
    //FILE* f = fopen(fname,"w+");
    //打开数据库
    int oflags = O_CREAT | O_RDWR;
    void *openinfo;
    BTREEINFO ib;
	ib.maxkeypage=4;
	ib.minkeypage=0;
    ib.cachesize = 65536;
    ib.psize=8*1024;
	ib.compare=NULL;
	ib.prefix=NULL;
	ib.lorder=0;
    ib.flags = R_DUP;
    openinfo = &ib;
    int len;
    char *p;
    FILE *file = fopen("lineitem.tbl","r");
	//FILE *file = fopen("testt.tbl","r");
    
    
    dbp = dbopen(fname,oflags,S_IRUSR | S_IWUSR,DB_BTREE,openinfo);
    if (dbp == NULL){
		printf("error\n");
		return 0;
	}
    //开始读取文件内容并插入btree
   /* int linenum = 1;
    for(;(p = fgets(buf, 8*1024,file)) != NULL;++linenum){
        len = strlen(buf);
        int count = 0;
        int i;
        for (i = 0;count != 10;i++)
        {
             if (buf[i] == '|')
                 count ++;
        }
		key.data = xmalloc(p+i,11);
        key.size = 11;
        data.data = xmalloc(p,len - 1);
        data.size = len;
        put(dbp,&key,&data);

    }*/
	//索引建立
   //to do: 建立第二个属性的索引,从原来文件中获取key值，从第一索引中查找相关的页号和槽号，然后再开第二个dbp，put就行啦！！！
   // linenum = 1;
	EPG e;
	DB *dbp2;
	DBT tempKey;
	DBT secondKey;
	DBT secondData;
	char *fname2 = "ind2";
	//char *fname2 = "test2";
	//FILE *ff = fopen(fname2,"w");
	int oflagss = O_CREAT | O_RDWR;
	file = fopen("testt.tbl","r");

    BTREEINFO ic;
	ic.maxkeypage=4;
	ic.minkeypage=0;
    ic.cachesize = 65536;
    ic.psize=8*1024;
	ic.compare=NULL;
	ic.prefix=NULL;
	ic.lorder=0;
    ic.flags = R_DUP;
    openinfo = &ic;
    dbp2 = dbopen(fname2,oflagss,S_IRUSR | S_IWUSR,DB_BTREE,openinfo);
    //用第一个key去找，返回一个页面，包含很多符合要求的记录，有可能一个Key对应很多个record	
	int num = 0;
	int j;
	int status;
	EPGNO ee;
	DBT tempData;
	char format[20];
/*	status =page__seq(dbp,&tempKey,&tempData,R_FIRST,&e);
	while (status == RET_SUCCESS){
        for (j = 0;num != 6;j ++){
			if (*(((char*)tempData.data)+j) == '|')
				num ++;
			}
         int fir = j;
		 for (;*(((char*)tempData.data)+j) != '|';j ++);
		 int k = j - fir;
		 char *pp = xmalloc((char*)tempData.data+fir,k + 1);
		 secondKey.data = pp;
		 secondKey.size = k + 1;
		 ee.pgno = (e.page)->pgno;
		 ee.index = e.index;
		 sprintf(format,"%d %d",ee.pgno,ee.index);
		 secondData.data = format;
		 secondData.size = strlen(format);
		 put(dbp2,&secondKey,&secondData);
		 printf("secondkey = %s  pagennum = %d  slot: %d\n",secondKey.data,ee.pgno,ee.index);
         status = page__seq(dbp,&tempKey,&tempData,R_NEXT,&e);
		 num = 0;
	}*/

    
    //索引2建立成功
   // 范围查询开始(方法一)----------------------------------------------------------------------------------------------------------------
   /* 方法大概为： 首先用Key2在tree2中遍历，找出符合条件的page，和slot，然后找出这些页，使用mpool_get去获得这些页，然后从页里去比较是否在范围内
	*/
	//遍历索引一，找到上限的位置
	FILE *qfile = fopen("query.in","r"); 
    char *rangeupper1 = malloc(11*sizeof(char));
	char *rangelower1 = malloc(11*sizeof(char));
	char *rangeupper2 = malloc(5*sizeof(char));
	char *rangelower2 = malloc(5*sizeof(char));
	char *quantitybound = malloc(2*sizeof(char));
	char buf_1[200];
	FILE *qout = fopen("query.out","w+");
/*	while (fgets(buf_1,200,qfile) != NULL){
	sscanf(buf_1,"%s %s %s %s %s",rangelower1,rangeupper1,rangelower2,rangeupper2,quantitybound);
	printf("%s %s %s %s %s\n",rangelower1,rangeupper1,rangelower2,rangeupper2,quantitybound);
	BTREE *t = dbp->internal;
	MPOOL *mp = t->bt_mp;
	int sum = 0;
	double total = 0;
	DBT rkey,rdata,key1,data1;
    DBT tempKey1,tempData1;
	
    tempKey1.data = xmalloc(rangelower1,strlen(rangelower1) + 1);
	tempKey1.size = strlen(rangelower1) + 1;
	tempKey1.data = rangelower1;
	tempKey1.size = strlen(rangelower1);
	status = page__seq(dbp,&tempKey1,&tempData1,R_CURSOR,&e);
	int first = 0;
	while (status == RET_SUCCESS){
        char *charr = tempData1.data;
		
		int jj = 0;
        //get the date
		int count2 = 0;
		for (;count2 != 10;jj ++){
			if (charr[jj] == '|')
				count2 ++;
		}
		int startpos = jj;
		for (;charr[jj] != '|';jj ++);
		char *date = xmalloc(charr+startpos,jj - startpos + 1);
		//printf("%s\n",date);
		if (strcmp(date,rangeupper1) >= 0){
			printf("break\n");
			break;
	   }
		first = 1;
		//printf("called\n");
        //printf("%s\n",tempData1.data);
		//get the discount
		jj = 0;
		count2 = 0;
		for (;count2 != 6;jj ++){
			if (charr[jj] == '|')
				count2 ++;
		}
		startpos = jj;
		for (;charr[jj] != '|';jj ++);
		char *dis = xmalloc(charr + startpos,jj - startpos + 1);

		//get the quantity
		count2 = 0;
		jj = 0;
	    for (;count2 != 4;jj++){
			if (charr[jj] == '|')
				 count2 ++;
	    }
		startpos = jj;
		for (;charr[jj] != '|';jj ++);
	    char *quantity = xmalloc(charr+startpos,jj - startpos + 1);

		//get the extendedprice
		count2 = 0;
	    startpos = ++jj;
		for (;charr[jj] != '|'; jj++);
		char *extend = xmalloc(charr + startpos,jj - startpos + 1);
		double extendedprice = atof(extend);
		int quu = atoi(quantity);
		int quu2 = atoi(quantitybound);
        if (strcmp(date,rangelower1) > 0 && strcmp(dis,rangeupper2) < 0 && strcmp(dis,rangelower2) > 0 && quu < quu2){
			sum ++;
        	double disount = atof(dis);
			printf("extendedprice = %lf discout = %lf extendedprice*discount = %lf\n",extendedprice,disount,extendedprice*disount);
			printf("data = %s\n",tempData1.data);
			total += extendedprice * disount;
		}
		status = page__seq(dbp,&tempKey1,&tempData1,R_NEXT,&e);
	}
	printf("done! total = %f  sum = %d\n",total,sum);
	if (total - 0.0 > 0.0001)
		fprintf(qout,"%lf\n",total);
	else
		fprintf(qout,"%s\n","NULL");

	}*/
	//范围查询方法二；
	//范围查询结束-----------------------------------------------------------------------
    

	//-------------------------------------------UB  TREE ----------------------------------------------
	//UB Tree 建立索引
    DB *dbp4;
	//char *fname3 = "UBtree";
	char *fname3 = "UBtree1";
	int oflagss_ = O_CREAT | O_RDWR;
    BTREEINFO ie;
	ie.maxkeypage=4;
	ie.minkeypage=0;
    ie.cachesize = 65536;
    ie.psize=8*1024;
	ie.compare=NULL;
	ie.prefix=NULL;
	ie.lorder=0;
    ie.flags = R_DUP;
	void *openinfo_;
    openinfo_ = &ie;
	FILE *Ufile = fopen("lineitem.tbl","r");
    dbp4 = dbopen(fname3,oflagss_,S_IRUSR | S_IWUSR,DB_BTREE,openinfo_);
   if (dbp4 == NULL)
	   printf("wrong!\n");
	DBT Ukey,UData;
    int linenum = 1;
	int k = 0;
   /* for(;(p = fgets(buf, 8*1024,Ufile)) != NULL;++linenum){
        len = strlen(buf);
		int  jj = 0;
        //get the date
		int count3 = 0;
		for (;count3 != 10;jj ++){
			if (buf[jj] == '|')
				count3 ++;
		}
		int startpos = jj;
		for (;buf[jj] != '|';jj ++);
		char *date = xmalloc(p+startpos,jj - startpos + 1);
		//get the discount
		jj = 0;
		count3 = 0;
		for (;count3 != 6;jj ++){
			if (buf[jj] == '|')
				count3 ++;
		}
		startpos = jj;
		for (;buf[jj] != '|';jj ++);
		char *dis = xmalloc(p + startpos,jj - startpos + 1);
		//get the quantity
		count3 = 0;
		jj = 0;
	    for (;count3 != 4;jj++){
			if (buf[jj] == '|')
				 count3 ++;
	    }
		startpos = jj;
		for (;buf[jj] != '|';jj ++);
	    char *quantity = xmalloc(p+startpos,jj - startpos + 1);
		//get the extendedprice
		count3 = 0;
	    startpos = ++jj;
		for (;buf[jj] != '|'; jj++);
		char *extend = xmalloc(p + startpos,jj - startpos + 1);
		double extendedprice = atof(extend);
		int quu = atoi(quantity);

		/*
		 *二进制转换函数
		 */
	   /* char *combine = malloc(42*sizeof(char));
		multi_str2bin_(combine,date,dis,quu);
        Ukey.data = combine;
		Ukey.size = 42;
		char *da = xmalloc(p,len+1);
		UData.data = da;
		UData.size = len + 1;
		put(dbp4,&Ukey,&UData);

   }
   printf("%d\n",k);*/

	//UBtree 点查询---------------------------------------------------------------------------
	char *Uqdate = "1994-01-01";
	char *Uqdis = "0.01";
	char *Uquan = "50";
	int Qu = atoi(Uquan);
	char *comthree = malloc(42*sizeof(char));
	multi_str2bin_(comthree,Uqdate,Uqdis,Qu);
	Ukey.data = comthree;
	printf("%s\n",comthree);
	Ukey.size = 42;
	double Usum = 0;
    int ss = 0;
	status = page__seq(dbp4,&Ukey,&UData,R_CURSOR,&e);
	while (status == RET_SUCCESS){
		char *Ud = UData.data;
		printf("%s\n",Ukey.data);
		char *d1 =malloc(26*sizeof(char));
		char *d2 =malloc(6*sizeof(char));
		char *d3 =malloc(9*sizeof(char));
		bin2three(Ukey.data,d1,d2,d3);
		printf("%s %s %s\n",d1,d2,d3);
		printf("%s\n",UData.data);
		//if the key is  not equal to Uqdate,break;
		if (strcmp(Ukey.data,comthree) != 0)
			break;
		//get the quantity
		else{
		int Ucount = 0;
		int jj = 0;
	    for (;Ucount != 4;jj++){
			if (Ud[jj] == '|')
				 Ucount ++;
	    }
		int startpos = jj;
		for (;Ud[jj] != '|';jj ++);
	    char *Uquantity = xmalloc(Ud+startpos,jj - startpos + 1);
		//get the extendedprice
		Ucount = 0;
	    startpos = ++jj;
		for (;Ud[jj] != '|'; jj++);
		char *Uextend = xmalloc(Ud + startpos,jj - startpos + 1);
		double Uextendedprice = atof(Uextend);
		Usum += Uextendedprice * atof(Uqdis);
		ss ++;
		status = page__seq(dbp4,&Ukey,&UData,R_NEXT,&e);
		}
	}

	printf("Uqurey done! the total is %lf %d\n",Usum,ss);

       //范围查询
	char *Uupper1 = "1995-01-01";
	char *Ulower1 = "1994-01-01";
	char *Uupper2 = "0.05";
	char *Ulower2 = "0.01";
	char *Uquantity = "30";
	int Uqu1 = atoi(Uquantity);
	int Uqu2 = 0;

	
	char *startKey = malloc(42*sizeof(char));
	char *endKey = malloc(42*sizeof(char));
	multi_str2bin_(startKey,Ulower1,Ulower2,Uqu2);
	multi_str2bin_(endKey,Uupper1,Uupper2,Uqu1);
	
	char *bin_up1 = malloc(27*sizeof(char));
	char *bin_low1 = malloc(27*sizeof(char));
	char *bin_up2 = malloc(9*sizeof(char));
	char *bin_low2 = malloc(9*sizeof(char));
	char *bin_up3 = malloc(9*sizeof(char));
	char *bin_low3 = malloc(9*sizeof(char));

	str2bin_(Uupper1,bin_up1);
	str2bin_(Uupper2,bin_up2);
	str2bin_(Ulower1,bin_low1);
	str2bin_(Ulower2,bin_low2);
	str2bin_(Uquantity,bin_up3);
	str2bin_("00",bin_low3);
	
	DBT UrKey,UrData;
	UrKey.data = startKey;
	printf("%s\n",startKey);

    char *dismenn1 = malloc(26*sizeof(char));
	char *dismenn2 = malloc(9*sizeof(char));
	char *dismenn3 = malloc(9*sizeof(char));
	
	bin2three(startKey,dismenn1,dismenn2,dismenn3);
	
	UrKey.size = 42;
    status = page__seq(dbp4,&UrKey,&UrData,R_CURSOR,&e);
    char *test = UrKey.data;
	bin2three(test,dismenn1,dismenn2,dismenn3);
	double Rtotal = 0;
	int Rsum = 0;
	int u = 0;
	while (status == RET_SUCCESS){
		char *Ud = UrData.data; 
	    
		if (strcmp((char*)UrKey.data,endKey) >= 0)
	    {	
			printf("break\n");
			break;
		}
		//---------------------filtering --------------------------
		
		//get the discount
		int jj = 0;
		int Ucount = 0;
		for (;Ucount != 6;jj ++){
			if (Ud[jj] == '|')
				Ucount ++;
		}
		int startpos = jj;
		for (;Ud[jj] != '|';jj ++);
		char *dis = xmalloc(Ud + startpos,jj - startpos + 1);
		//get the quantity
		Ucount = 0;
		jj = 0;
	    for (;Ucount != 4;jj++){
			if (Ud[jj] == '|')
				 Ucount ++;
	    }
		startpos = jj;
		for (;Ud[jj] != '|';jj ++);
	    //char *Uquan = xmalloc(Ud+startpos,jj - startpos + 1);
		//int Uq2 = atoi(Uquan);
		//get the extendedprice
		Ucount = 0;
	    startpos = ++jj;
		for (;Ud[jj] != '|'; jj++);
		char *Uextend = xmalloc(Ud + startpos,jj - startpos + 1);
        
		char *dismen1 = malloc(26*sizeof(char));
		char *dismen2 = malloc(9*sizeof(char));
		char *dismen3 = malloc(9*sizeof(char));
		char *Utemp = UrKey.data;
		bin2three(Utemp,dismen1,dismen2,dismen3);
		if(strcmp(dismen1,bin_up1) < 0 && strcmp(dismen1,bin_low1) > 0 && strcmp(dismen2,bin_up2) < 0 && strcmp(dismen2,bin_low2) > 0 &&  strcmp(dismen3,bin_up3) < 0 && strcmp(dismen3,bin_low3) > 0)
		{
			double Uextendedprice = atof(Uextend);
			Rtotal += Uextendedprice * atof(dis);
			Rsum ++;
		}
		DBT tKey = UrKey;
		status = page__seq(dbp4,&UrKey,&UrData,R_NEXT,&e);
		// 比较这时的key是否在范围内，不是则进行找nisp,是则继续往下查询
		Utemp = UrKey.data;
		bin2three(Utemp,dismen1,dismen2,dismen3);
		if (strcmp(dismen1,bin_up1) <= 0 && strcmp(dismen1,bin_low1) >= 0 && strcmp(dismen2,bin_up2) <= 0 && strcmp(dismen2,bin_low2) >= 0 &&  strcmp(dismen3,bin_up3) <= 0 && strcmp(dismen3,bin_low3) >= 0)
		{
			continue;
		}
		else
		{
			//开始找一下个开始点
			char *Utempp = UrKey.data;
			
			int flag[4];
			int outstep[4];
			int savemin[4];
			int savemax[4];
			//----------------第一维------------------------
			 if (strcmp(dismen1,bin_up1) > 0)
				flag[1] = 1;
			 else if (strcmp(dismen1,bin_low1) < 0)
				flag[1] = -1;
			 else
				flag[1] = 0;
				
			 information(flag[1],&savemin[1],&savemax[1],&outstep[1],dismen1,bin_up1,bin_low1);
            //-----------------第二维-----------------------
			if (strcmp(dismen2,bin_up2) > 0)
				flag[2] =1;
			else if (strcmp(dismen2,bin_low2) < 0)
				flag[2] = -1;
			else
				flag[2] = 0;

			information(flag[2],&savemin[2],&savemax[2],&outstep[2],dismen2,bin_up2,bin_low2);
            //-----------------第三维-----------------------
			if (strcmp(dismen3,bin_up3) > 0)
				flag[3] =1;
			else if (strcmp(dismen3,bin_low3) < 0)
				flag[3] = -1;
			else
				flag[3] = 0;

			information(flag[3],&savemin[3],&savemax[3],&outstep[3],dismen3,bin_up3,bin_low3);

			//-------------各维提取相关信息完毕-----------------------
			int haveFound = 0;
			position p;
			int tobeChange;
			if (outstep[1] > outstep[2])
			{
				if (outstep[3] > outstep[2])
				{
					if (dismen2[outstep[2]] == '0')
					{
						dismen2[outstep[2]] == '1';
						haveFound = 1;
					}
					p.dimension = 2;
					p.index = outstep[2];
				}
				else
				{
					if (dismen3[outstep[3]] == '0')
					{
						dismen3[outstep[3]] == '1';
						haveFound = 1;
					}
					p.dimension = 3;
					p.index = outstep[3];
				}
			}
			else
			{
				if (outstep[3] > outstep[1])
				{
					if (dismen1[outstep[1]] == '0')
					{
						dismen1[outstep[1]] == '1';
						haveFound = 1;
					}
					p.dimension = 1;
					p.index = outstep[1];
				}
				else
				{
					if (dismen3[outstep[3]] == '0')
					{
						dismen3[outstep[3]] == '1';
						haveFound = 1;
					}
					p.dimension = 3;
					p.index = outstep[3];
				}
			}
				
	
			if (!haveFound){
				int changebp = find_pos_in3(p);
                int j = changebp;
				char *k = UrKey.data;
				for (;j >= 0;j --){
					if (k[j] == '0'){
						position p1 = where_in3(j);
						if (p1.index >= savemax[p1.dimension]){
							tobeChange = j;
							break;
						}
					}
				}
			}
			else
				tobeChange = find_pos_in3(p);
			
			Utempp[tobeChange] = '1';
			position po = where_in3(tobeChange);
			if (po.dimension == 1)
				dismen1[po.index] = '1';
			else if (po.dimension == 2)
				dismen2[po.index] = '1';
			else
				dismen3[po.index] = '1';
            
			
			information(flag[1],&savemin[1],&savemax[1],&outstep[1],dismen1,bin_up1,bin_low1);
            information(flag[2],&savemin[2],&savemax[2],&outstep[2],dismen2,bin_up2,bin_low2);
			information(flag[3],&savemin[3],&savemax[3],&outstep[3],dismen3,bin_up3,bin_low3);

			//找到把0变成1的点，下面开始各维度调整----------------------
			//第一维：
			if (flag[1] == -1)
			{	
				strcpy(dismen1,bin_low1);
				int ii;
				for (ii = 0;ii < strlen(dismen1);ii ++){
					if (ii < 8)
						Utempp[ii * 3] = dismen1[ii];
					else
						Utempp[ii + 16] = dismen1[ii];
				}
			}
			else{
				position BP;
				BP.dimension = 1;
				BP.index = savemin[1];
				int pos = find_pos_in3(BP);
				position P = where_in3(tobeChange);
				if (tobeChange >= pos){
					int start = 0;
					for (;start < strlen(dismen1) ;start ++){
						position poss;
						poss.dimension = 1;
						poss.index = start;
						int loca = find_pos_in3(poss);
						if (loca > tobeChange){
							dismen1[start] = '0';
							if (start < 8)
								Utempp[start*3] = '0';
							else
								Utempp[start+16] = '0';
						}
					}

				}
				else{
					int start = 0;
					for (;start < strlen(dismen1); start ++){
						position poss;
						poss.dimension = 1;
						poss.index = start;
						int loca = find_pos_in3(poss);
						if (loca > tobeChange){
							dismen1[start] = bin_low1[start];
							if (start < 8)
								Utempp[start*3] = bin_low1[start];
							else
								Utempp[start+16] = bin_low1[start];
						}
					}
				}
			}
			//第二维

			if (flag[2] == -1)
			{	
				strcpy(dismen2,bin_low2);
				int ii;
				for (ii = 0;ii < strlen(dismen2);ii ++){
					if (ii < 8)
						Utempp[ii*3 + 1] = dismen2[ii];
					else
						Utempp[ii + 16] = dismen2[ii];
				}
			}
			else{
				position BP;
				BP.dimension = 2;
				BP.index = savemin[2];
				int pos = find_pos_in3(BP);
				position P = where_in3(tobeChange);
				if (tobeChange >= pos){
					int start = 0;
					for (;start < strlen(dismen2) ;start ++){
						position poss;
						poss.dimension = 2;
						poss.index = start;
						int loca = find_pos_in3(poss);
						if (loca > tobeChange){
							dismen2[start] = '0';
							if (start < 8)
								Utempp[start*3 + 1] = '0';
							else
								Utempp[start+16] = '0';
						}
					}
				}
				else{
					int start = 0;
					for (;start < strlen(dismen2); start ++){
						position poss;
						poss.dimension = 2;
						poss.index = start;
						int loca = find_pos_in3(poss);
						if (loca > tobeChange){
							dismen2[start] = bin_low2[start];
							if (start < 8)
								Utempp[start*3 + 1] = bin_low2[start];
							else
								Utempp[start+16] = bin_low2[start];
						}
					}
				}
			}

			// 第三维
			if (flag[3] == -1)
			{	
				strcpy(dismen3,bin_low3);
				int ii;
				for (ii = 0;ii < strlen(dismen3);ii ++){
					if (ii < 8)
						Utempp[ii*3 + 2] = dismen3[ii];
					else
						Utempp[ii + 16] = dismen3[ii];
				}
			}
			else{
				position BP;
				BP.dimension = 3;
				BP.index = savemin[3];
				int pos = find_pos_in3(BP);
				position P = where_in3(tobeChange);
				if (tobeChange >= pos){
					int start = 0;
					for (;start < strlen(dismen3) ;start ++){
						position poss;
						poss.dimension = 3;
						poss.index = start;
						int loca = find_pos_in3(poss);
						if (loca > tobeChange){
							dismen3[start] = '0';
							if (start < 8)
								Utempp[start*3 + 2] = '0';
							else
								Utempp[start+16] = '0';
						}
					}
				}
				else{
					int start = 0;
					for (;start < strlen(dismen3); start ++){
						position poss;
						poss.dimension = 3;
						poss.index = start;
						int loca = find_pos_in3(poss);
						if (loca > tobeChange){
							dismen3[start] = bin_low3[start];
							if (start < 8)
								Utempp[start*3 + 2] = bin_low3[start];
							else
								Utempp[start+16] = bin_low3[start];
						}
					}
				}
			}
		    UrKey.data = Utempp;
		    UrKey.size = 42;
			status = page__seq(dbp4,&UrKey,&UrData,R_CURSOR,&e);
			free(dismen1);
			free(dismen2);
			free(dismen3);
		}
	}

    printf("UBtree range query done! the total is %lf   there are %d records\n",Rtotal,Rsum);

   dbp4->close(dbp4);
   dbp2->close(dbp2);
   dbp->close(dbp);

   return 0;
}

void information(int flag,int *savemin,int *savemax, int *outstep,char *dismen,char *up,char *low)
{

	int i = 0;
	if (flag == 1){
		for (;i < strlen(dismen);i ++){
			if (dismen[i] > up[i]){
				*outstep = i;
				*savemax = maxxx;
				break;
			}
		}
		for (i = 0;i < strlen(dismen);i ++){
			if (dismen[i] > low[i]){
				*savemin = i;
				break;
			}
		}
	}
	else if (flag == -1){
		for (i = 0;i < strlen(dismen);i ++){
			if (dismen[i] < low[i]){
				*outstep = i;
				*savemin = maxxx;
				break;
			}
		}
	    for (i = 0;i < strlen(dismen);i ++){
			if (dismen[i] < up[i]){
				*savemax = i;
				break;
			}
		}
	}
	else{
		*outstep = maxxx;
		for (i = 0;i <strlen(dismen);i ++){
			if (dismen[i] > low[i]){
				*savemin = i;
				break;
			}
		}
		if (i == strlen(dismen))
		   *savemin = maxxx;
		for (i = 0;i < strlen(dismen);i ++){
			if (dismen[i] < up[i]){
				*savemax = i;
				break;
			}
		}
		if (i == strlen(dismen))
			*savemax = maxxx;
	}
}





int
page__seq(const DB *dbp, DBT *key, DBT *data, u_int flags, EPG *e)
{
	BTREE *t;
	int status;

	t = dbp->internal;

	if (t->bt_pinned != NULL) {
		mpool_put(t->bt_mp, t->bt_pinned, 0);
		t->bt_pinned = NULL;
	}

  
        
        
	switch (flags) {
	case R_NEXT:
	case R_PREV:
		if (F_ISSET(&t->bt_cursor, CURS_INIT)) {
			status = __btr_seqadv(t, e, flags);
			break;
		}
	case R_FIRST:
	case R_LAST:
	case R_CURSOR:
		status = __btr_seqset(t, e, key, flags);
		break;
	default:
		errno = EINVAL;
		return status;
	}

	if (status == RET_SUCCESS) {
		__bt_setcur(t, e->page->pgno, e->index);

		status =
		    __bt_ret(t, e, key, &t->bt_rkey, data, &t->bt_rdata, 0);

		if (F_ISSET(t, B_DB_LOCK))
			mpool_put(t->bt_mp, e->page, 0);
		else
			t->bt_pinned = e->page;
	}
	return status;
}

static int
__btr_seqset(t, ep, key, flags)
	BTREE *t;
	EPG *ep;
	DBT *key;
	int flags;
{
	PAGE *h;
	pgno_t pg;
	int exact;

	/*
	 * Find the first, last or specific key in the tree and point the
	 * cursor at it.  The cursor may not be moved until a new key has
	 * been found.
	 */
        
	switch (flags) {
	case R_CURSOR:				/* Keyed scan. */
		/*
		 * Find the first instance of the key or the smallest key
		 * which is greater than or equal to the specified key.
		 */
		if (key->data == NULL || key->size == 0) {
			errno = EINVAL;
			return (RET_ERROR);
		}
		return (__btr_first(t, key, ep, &exact));
	case R_FIRST:				/* First record. */
	case R_NEXT:
		/* Walk down the left-hand side of the tree. */
		for (pg = P_ROOT;;) {
			if ((h = mpool_get(t->bt_mp, pg, 0)) == NULL)
				return (RET_ERROR);

			/* Check for an empty tree. */
			if (NEXTINDEX(h) == 0) {
				mpool_put(t->bt_mp, h, 0);
				return (RET_SPECIAL);
			}

			if (h->flags & (P_BLEAF | P_RLEAF))
				break;
			pg = GETBINTERNAL(h, 0)->pgno;
			mpool_put(t->bt_mp, h, 0);
		}
		ep->page = h;
		ep->index = 0;
		break;
	case R_LAST:				/* Last record. */
	case R_PREV:
		/* Walk down the right-hand side of the tree. */
		for (pg = P_ROOT;;) {
			if ((h = mpool_get(t->bt_mp, pg, 0)) == NULL)
				return (RET_ERROR);

			/* Check for an empty tree. */
			if (NEXTINDEX(h) == 0) {
				mpool_put(t->bt_mp, h, 0);
				return (RET_SPECIAL);
			}

			if (h->flags & (P_BLEAF | P_RLEAF))
				break;
			pg = GETBINTERNAL(h, NEXTINDEX(h) - 1)->pgno;
			mpool_put(t->bt_mp, h, 0);
		}

		ep->page = h;
		ep->index = NEXTINDEX(h) - 1;
		break;
	}
	return (RET_SUCCESS);
}

static int
__btr_seqadv(t, ep, flags)
	BTREE *t;
	EPG *ep;
	int flags;
{
	CURSOR *c;
	PAGE *h;
	indx_t index;
	pgno_t pg;
	int exact;

	/*
	 * There are a couple of states that we can be in.  The cursor has
	 * been initialized by the time we get here, but that's all we know.
	 */
	c = &t->bt_cursor;

	/*
	 * The cursor was deleted where there weren't any duplicate records,
	 * so the key was saved.  Find out where that key would go in the
	 * current tree.  It doesn't matter if the returned key is an exact
	 * match or not -- if it's an exact match, the record was added after
	 * the delete so we can just return it.  If not, as long as there's
	 * a record there, return it.
	 */
	if (F_ISSET(c, CURS_ACQUIRE))
		return (__btr_first(t, &c->key, ep, &exact));

	/* Get the page referenced by the cursor. */
	if ((h = mpool_get(t->bt_mp, c->pg.pgno, 0)) == NULL)
		return (RET_ERROR);

	/*
 	 * Find the next/previous record in the tree and point the cursor at
	 * it.  The cursor may not be moved until a new key has been found.
	 */
	switch (flags) {
	case R_NEXT:			/* Next record. */
		/*
		 * The cursor was deleted in duplicate records, and moved
		 * forward to a record that has yet to be returned.  Clear
		 * that flag, and return the record.
		 */
		if (F_ISSET(c, CURS_AFTER))
			goto usecurrent;
		index = c->pg.index;
		if (++index == NEXTINDEX(h)) {
			pg = h->nextpg;
			mpool_put(t->bt_mp, h, 0);
			if (pg == P_INVALID)
				return (RET_SPECIAL);
			if ((h = mpool_get(t->bt_mp, pg, 0)) == NULL)
				return (RET_ERROR);
			index = 0;
		}
		break;
	case R_PREV:			/* Previous record. */
		/*
		 * The cursor was deleted in duplicate records, and moved
		 * backward to a record that has yet to be returned.  Clear
		 * that flag, and return the record.
		 */
		if (F_ISSET(c, CURS_BEFORE)) {
usecurrent:		F_CLR(c, CURS_AFTER | CURS_BEFORE);
			ep->page = h;
			ep->index = c->pg.index;
			return (RET_SUCCESS);
		}
		index = c->pg.index;
		if (index == 0) {
			pg = h->prevpg;
			mpool_put(t->bt_mp, h, 0);
			if (pg == P_INVALID)
				return (RET_SPECIAL);
			if ((h = mpool_get(t->bt_mp, pg, 0)) == NULL)
				return (RET_ERROR);
			index = NEXTINDEX(h) - 1;
		} else
			--index;
		break;
	}

	ep->page = h;
	ep->index = index;
	return (RET_SUCCESS);
}

static int
__btr_first(t, key, erval, exactp)
	BTREE *t;
	const DBT *key;
	EPG *erval;
	int *exactp;
{
	PAGE *h;
	EPG *ep, save;
	pgno_t pg;

	/*
	 * Find any matching record; __bt_search pins the page.
	 *
	 * If it's an exact match and duplicates are possible, walk backwards
	 * in the tree until we find the first one.  Otherwise, make sure it's
	 * a valid key (__bt_search may return an index just past the end of a
	 * page) and return it.
	 */
	if ((ep = __bt_search(t, key, exactp)) == NULL)
		return (NULL);
	if (*exactp) {
		if (F_ISSET(t, B_NODUPS)) {
			*erval = *ep;
                        printf("called\n");
			return (RET_SUCCESS);
		}
			
		/*
		 * Walk backwards, as long as the entry matches and there are
		 * keys left in the tree.  Save a copy of each match in case
		 * we go too far.
		 */
		save = *ep;
		h = ep->page;
		do {
			if (save.page->pgno != ep->page->pgno) {
				mpool_put(t->bt_mp, save.page, 0);
				save = *ep;
			} else
				save.index = ep->index;

			/*
			 * Don't unpin the page the last (or original) match
			 * was on, but make sure it's unpinned if an error
			 * occurs.
			 */
            
			if (ep->index == 0) {
				if (h->prevpg == P_INVALID)
					break;
				if (h->pgno != save.page->pgno)
					mpool_put(t->bt_mp, h, 0);
				if ((h = mpool_get(t->bt_mp,
				    h->prevpg, 0)) == NULL) {
					if (h->pgno == save.page->pgno)
						mpool_put(t->bt_mp,
						    save.page, 0);
					return (RET_ERROR);
				}
				ep->page = h;
				ep->index = NEXTINDEX(h);
			}
			--ep->index;
		} while (__bt_cmp(t, key, ep) == 0);

		/*
		 * Reach here with the last page that was looked at pinned,
		 * which may or may not be the same as the last (or original)
		 * match page.  If it's not useful, release it.
		 */
		if (h->pgno != save.page->pgno)
			mpool_put(t->bt_mp, h, 0);

		*erval = save;
		return (RET_SUCCESS);
	}

	/* If at the end of a page, find the next entry. */
	if (ep->index == NEXTINDEX(ep->page)) {
		h = ep->page;
		pg = h->nextpg;
		mpool_put(t->bt_mp, h, 0);
		if (pg == P_INVALID)
			return (RET_SPECIAL);
		if ((h = mpool_get(t->bt_mp, pg, 0)) == NULL)
			return (RET_ERROR);
		ep->index = 0;
		ep->page = h;
	}
	*erval = *ep;
	return (RET_SUCCESS);
}
#define	NOOVERWRITE	"put failed, would overwrite key\n"

void
compare(db1, db2)
	DBT *db1, *db2;
{
	register size_t len;
	register u_char *p1, *p2;

	if (db1->size != db2->size)
		printf("compare failed: key->data len %lu != data len %lu\n",
		    db1->size, db2->size);

	len = MIN(db1->size, db2->size);
	for (p1 = db1->data, p2 = db2->data; len--;)
		if (*p1++ != *p2++) {
			printf("compare failed at offset %d\n",
			    p1 - (u_char *)db1->data);
			break;
		}
}

void
get(dbp, kp)
	DB *dbp;
	DBT *kp;
{
	DBT data;

	switch (dbp->get(dbp, kp, &data, flags)) {
	case 0:
		
		(void)write(ofd, data.data, data.size);
		if (ofd == STDOUT_FILENO)
			(void)write(ofd, "\n", 1);
		break;
	case -1:
		err("line %lu: get: %s", lineno, strerror(errno));
		/* NOTREACHED */
	case 1:
#define	NOSUCHKEY	"get failed, no such key\n"
		if (ofd != STDOUT_FILENO)
			(void)write(ofd, NOSUCHKEY, sizeof(NOSUCHKEY) - 1);
		else
			(void)fprintf(stderr, "%d: %.*s: %s",
			    lineno, MIN(kp->size, 20), kp->data, NOSUCHKEY);
#undef	NOSUCHKEY
		break;
	}
}

void
getdata(dbp, kp, dp)
	DB *dbp;
	DBT *kp, *dp;
{
	switch (dbp->get(dbp, kp, dp, flags)) {
	case 0:
		return;
	case -1:
		err("line %lu: getdata: %s", lineno, strerror(errno));
		/* NOTREACHED */
	case 1:
		err("line %lu: getdata failed, no such key", lineno);
		/* NOTREACHED */
	}
}

void
put(dbp, kp, dp)
	DB *dbp;
	DBT *kp, *dp;
  
{
        
	switch (dbp->put(dbp, kp, dp, flags)) {
	case 0:
		
		break;
	case -1:
		err("line %lu: put: %s", lineno, strerror(errno));
		/* NOTREACHED */
	case 1:
		
		(void)write(ofd, NOOVERWRITE, sizeof(NOOVERWRITE) - 1);
		break;
	}
}

void
rem(dbp, kp)
	DB *dbp;
	DBT *kp;
{
	switch (dbp->del(dbp, kp, flags)) {
	case 0:
		break;
	case -1:
		err("line %lu: rem: %s", lineno, strerror(errno));
		/* NOTREACHED */
	case 1:
#define	NOSUCHKEY	"rem failed, no such key\n"
		if (ofd != STDOUT_FILENO)
			(void)write(ofd, NOSUCHKEY, sizeof(NOSUCHKEY) - 1);
		else if (flags != R_CURSOR)
			(void)fprintf(stderr, "%d: %.*s: %s",
			    lineno, MIN(kp->size, 20), kp->data, NOSUCHKEY);
		else
			(void)fprintf(stderr,
			    "%d: rem of cursor failed\n", lineno);
#undef	NOSUCHKEY
		break;
	}
}

void
synk(dbp)
	DB *dbp;
{
	switch (dbp->sync(dbp, flags)) {
	case 0:
		break;
	case -1:
		err("line %lu: synk: %s", lineno, strerror(errno));
		/* NOTREACHED */
	}
}

void
seq(dbp, kp)
	DB *dbp;
	DBT *kp;
{
	DBT data;

	switch (dbp->seq(dbp, kp, &data, flags)) {
	case 0:
		(void)write(ofd, data.data, data.size);
		if (ofd == STDOUT_FILENO)
			(void)write(ofd, "\n", 1);
		break;
	case -1:
		err("line %lu: seq: %s", lineno, strerror(errno));
		/* NOTREACHED */
	case 1:
#define	NOSUCHKEY	"seq failed, no such key\n"
		if (ofd != STDOUT_FILENO)
			(void)write(ofd, NOSUCHKEY, sizeof(NOSUCHKEY) - 1);
		else if (flags == R_CURSOR)
			(void)fprintf(stderr, "%d: %.*s: %s",
			    lineno, MIN(kp->size, 20), kp->data, NOSUCHKEY);
		else
			(void)fprintf(stderr,
			    "%d: seq (%s) failed\n", lineno, sflags(flags));
#undef	NOSUCHKEY
		break;
	}
}

void
dump(dbp, rev)
	DB *dbp;
	int rev;
{
	DBT key, data;
	int flags, nflags;

	if (rev) {
		flags = R_LAST;
		nflags = R_PREV;
	} else {
		flags = R_FIRST;
		nflags = R_NEXT;
	}
	for (;; flags = nflags)
		switch (dbp->seq(dbp, &key, &data, flags)) {
		case 0:
			(void)write(ofd, data.data, data.size);
			if (ofd == STDOUT_FILENO)
				(void)write(ofd, "\n", 1);
			break;
		case 1:
			goto done;
		case -1:
			err("line %lu: (dump) seq: %s",
			    lineno, strerror(errno));
			/* NOTREACHED */
		}
done:	return;
}

u_int
setflags(s)
	char *s;
{
	char *p, *index();

	for (; isspace(*s); ++s);
	if (*s == '\n' || *s == '\0')
		return (0);
	if ((p = index(s, '\n')) != NULL)
		*p = '\0';
	if (!strcmp(s, "R_CURSOR"))		return (R_CURSOR);
	if (!strcmp(s, "R_FIRST"))		return (R_FIRST);
	if (!strcmp(s, "R_IAFTER")) 		return (R_IAFTER);
	if (!strcmp(s, "R_IBEFORE")) 		return (R_IBEFORE);
	if (!strcmp(s, "R_LAST")) 		return (R_LAST);
	if (!strcmp(s, "R_NEXT")) 		return (R_NEXT);
	if (!strcmp(s, "R_NOOVERWRITE"))	return (R_NOOVERWRITE);
	if (!strcmp(s, "R_PREV"))		return (R_PREV);
	if (!strcmp(s, "R_SETCURSOR"))		return (R_SETCURSOR);

	err("line %lu: %s: unknown flag", lineno, s);
	/* NOTREACHED */
}

char *
sflags(flags)
	int flags;
{
	switch (flags) {
	case R_CURSOR:		return ("R_CURSOR");
	case R_FIRST:		return ("R_FIRST");
	case R_IAFTER:		return ("R_IAFTER");
	case R_IBEFORE:		return ("R_IBEFORE");
	case R_LAST:		return ("R_LAST");
	case R_NEXT:		return ("R_NEXT");
	case R_NOOVERWRITE:	return ("R_NOOVERWRITE");
	case R_PREV:		return ("R_PREV");
	case R_SETCURSOR:	return ("R_SETCURSOR");
	}

	return ("UNKNOWN!");
}

DBTYPE
dbtype(s)
	char *s;
{
	if (!strcmp(s, "btree"))
		return (DB_BTREE);
	if (!strcmp(s, "hash"))
		return (DB_HASH);
	if (!strcmp(s, "recno"))
		return (DB_RECNO);
	err("%s: unknown type (use btree, hash or recno)", s);
	/* NOTREACHED */
}

void *
setinfo(type, s)
	DBTYPE type;
	char *s;
{
	static BTREEINFO ib;
	static HASHINFO ih;
	static RECNOINFO rh;
	char *eq, *index();

	if ((eq = index(s, '=')) == NULL)
		err("%s: illegal structure set statement", s);
	*eq++ = '\0';
	if (!isdigit(*eq))
		err("%s: structure set statement must be a number", s);

	switch (type) {
	case DB_BTREE:
		if (!strcmp("flags", s)) {
			ib.flags = atoi(eq);
			return (&ib);
		}
		if (!strcmp("cachesize", s)) {
			ib.cachesize = atoi(eq);
			return (&ib);
		}
		if (!strcmp("maxkeypage", s)) {
			ib.maxkeypage = atoi(eq);
			return (&ib);
		}
		if (!strcmp("minkeypage", s)) {
			ib.minkeypage = atoi(eq);
			return (&ib);
		}
		if (!strcmp("lorder", s)) {
			ib.lorder = atoi(eq);
			return (&ib);
		}
		if (!strcmp("psize", s)) {
			ib.psize = atoi(eq);
			return (&ib);
		}
		break;
	case DB_HASH:
		if (!strcmp("bsize", s)) {
			ih.bsize = atoi(eq);
			return (&ih);
		}
		if (!strcmp("ffactor", s)) {
			ih.ffactor = atoi(eq);
			return (&ih);
		}
		if (!strcmp("nelem", s)) {
			ih.nelem = atoi(eq);
			return (&ih);
		}
		if (!strcmp("cachesize", s)) {
			ih.cachesize = atoi(eq);
			return (&ih);
		}
		if (!strcmp("lorder", s)) {
			ih.lorder = atoi(eq);
			return (&ih);
		}
		break;
	case DB_RECNO:
		if (!strcmp("flags", s)) {
			rh.flags = atoi(eq);
			return (&rh);
		}
		if (!strcmp("cachesize", s)) {
			rh.cachesize = atoi(eq);
			return (&rh);
		}
		if (!strcmp("lorder", s)) {
			rh.lorder = atoi(eq);
			return (&rh);
		}
		if (!strcmp("reclen", s)) {
			rh.reclen = atoi(eq);
			return (&rh);
		}
		if (!strcmp("bval", s)) {
			rh.bval = atoi(eq);
			return (&rh);
		}
		if (!strcmp("psize", s)) {
			rh.psize = atoi(eq);
			return (&rh);
		}
		break;
	}
	err("%s: unknown structure value", s);
	/* NOTREACHED */
}

void *
rfile(name, lenp)
	char *name;
	size_t *lenp;
{
	struct stat sb;
	void *p;
	int fd;
	char *np, *index();

	for (; isspace(*name); ++name);
	if ((np = index(name, '\n')) != NULL)
		*np = '\0';
	if ((fd = open(name, O_RDONLY, 0)) < 0 ||
	    fstat(fd, &sb))
    	err("%s: %s\n", name, strerror(errno));
#ifdef NOT_PORTABLE
	if (sb.st_size > (off_t)SIZE_T_MAX)
		err("%s: %s\n", name, strerror(E2BIG));
#endif
	if ((p = (void *)malloc((u_int)sb.st_size)) == NULL)
		err("%s", strerror(errno));
	(void)read(fd, p, (int)sb.st_size);
	*lenp = sb.st_size;
	(void)close(fd);
	return (p);
}

void *
xmalloc(text, len)
	char *text;
	size_t len;
{
   char* p;

	if ((p = (char *)malloc(len)) == NULL)
		err("%s", strerror(errno));
	memmove(p, text, len);
	p[len-1]='\0';
	return (p);
}

void
usage()
{
	(void)fprintf(stderr,
	    "usage: dbtest [-l] [-f file] [-i info] [-o file] type script\n");
	exit(1);
}
#if __STDC__
#include <stdarg.h>
#else
#include <varargs.h>
#endif

void
#if __STDC__
err(const char *fmt, ...)
#else
err(fmt, va_alist)
	char *fmt;
        va_dcl
#endif
{
	va_list ap;
#if __STDC__
	va_start(ap, fmt);
#else
	va_start(ap);
#endif
	(void)fprintf(stderr, "dbtest: ");
	(void)vfprintf(stderr, fmt, ap);
	va_end(ap);
	(void)fprintf(stderr, "\n");
	exit(1);
	/* NOTREACHED */
}

