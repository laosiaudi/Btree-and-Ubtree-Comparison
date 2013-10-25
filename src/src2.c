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
#include "btree.h"
#include "charToBi.c"
#include "bin2two.c"
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
void getNextValue(char *nisp,char *bin_low1,char *bin_up1,char *bin_low2,char *bin_up2,DBT UrKey);
DBTYPE type;				/* Database type. */
void *infop;				/* Iflags. */
u_long lineno;				/* Current line in test script. */
u_int flags;				/* Current DB flags. */
int ofd = STDOUT_FILENO;		/* Standard output fd. */
#define maxxx  99999;
int
main(int argc,char *argv[])
	
{

DB *XXdbp;				/* Global for gdb. */
int XXlineno;				/* Fast breakpoint for gdb. */
 
    DBT data, key, keydata;
    char buf_1[8*1024];
	char buf[200];
   char fname[6] = "UIndex";
   
   char path[200];
   strcpy(path,argv[1]);
   strcat(path,fname);
    //打开数据库
   
    char filename[100] = "lineitem.tbl";
    char path1[200];
    strcpy(path1,argv[1]);
    strcat(path1,filename);
    FILE *file = fopen(path1,"r");
    char *p;
	int len;
    char path4[200];
    char nn[100] = "query.in";
	char end[100] = "query.out";
	strcpy(path4,argv[1]);
	strcat(path4,nn);
	FILE *qfile = fopen(path4,"r");
    char path5[200];
	strcpy(path5,argv[1]);
	strcat(path5,end);
	FILE *qout = fopen(path5,"w+");

	//-------------------------------------------UB  TREE ----------------------------------------------
	//UB Tree 建立索引
   DB *dbp3;
	int oflagss_ = O_CREAT | O_RDWR;
    BTREEINFO id;
	id.maxkeypage=4;
	id.minkeypage=0;
    id.cachesize = 1048576;
    id.psize=8*1024;
	id.compare=NULL;
	id.prefix=NULL;
	id.lorder=0;
    id.flags = R_DUP;
	void *openinfo_;
    openinfo_ = &id;
    dbp3 = dbopen(path,oflagss_,S_IRUSR | S_IWUSR,DB_BTREE,openinfo_);
    DBT Ukey,UData;
    int linenum = 1;
	int status;
	EPG e;
    for(;(p = fgets(buf, 8*1024,file)) != NULL;++linenum){
        len = strlen(buf);
        
		int  jj = 0;
        //get the date
		int count3 = 0;
		for (;count3 != 10;jj ++){
			if (p[jj] == '|')
				count3 ++;
		}
		int startpos = jj;
		for (;p[jj] != '|';jj ++);
		char *date = xmalloc(p+startpos,jj - startpos + 1);
		//get the discount
		jj = 0;
		count3 = 0;
		for (;count3 != 6;jj ++){
			if (p[jj] == '|')
				count3 ++;
		}
		startpos = jj;
		for (;p[jj] != '|';jj ++);
		char *dis = xmalloc(p + startpos,jj - startpos + 1);

		//get the quantity
		count3 = 0;
		jj = 0;
	    for (;count3 != 4;jj++){
			if (p[jj] == '|')
				 count3 ++;
	    }
		startpos = jj;
		for (;p[jj] != '|';jj ++);
	    char *quantity = xmalloc(p+startpos,jj - startpos + 1);

		//get the extendedprice
		count3 = 0;
	    startpos = ++jj;
		for (;p[jj] != '|'; jj++);
		char *extend = xmalloc(p + startpos,jj - startpos + 1);
		double extendedprice = atof(extend);
		int quu = atoi(quantity);

		/*
		 *二进制转换函数
		 */
	    char *combine = malloc(37*sizeof(char));
		multi_str2bin(combine,date,dis);
        Ukey.data = combine;
		Ukey.size = 37;
		char *da = xmalloc(p,len+1);
		UData.data = da;
		UData.size = len;
		put(dbp3,&Ukey,&UData);

	}
    while(fgets(buf_1,200,qfile) != NULL){
	
	int bufLen = strlen(buf_1);
	if (bufLen < 20){
		char *Uqdate = malloc(11*sizeof(char));
		char *Uqdis = malloc(5*sizeof(char));
		char *Uquan = malloc(4*sizeof(char));
		
		sscanf(buf_1,"%s %s %s",Uqdate,Uqdis,Uquan);
	//UBtree 点查询---------------------------------------------------------------------------
		char *comtwo = malloc(37*sizeof(char));
		multi_str2bin(comtwo,Uqdate,Uqdis);
		Ukey.data = comtwo;
		Ukey.size = 37;
		double Usum = 0;
		status = page__seq(dbp3,&Ukey,&UData,R_CURSOR,&e);
		while (status == RET_SUCCESS){
			char *Ud = UData.data;
			//if the key is  not equal to Uqdate,break;
			if (strcmp(Ukey.data,comtwo) != 0)
				break;
			//get the quantity
			int Ucount = 0;
			int jj = 0;
	    	for (;Ucount != 4;jj++){
				if (Ud[jj] == '|')
					 Ucount ++;
	    	}
			int startpos = jj;
			for (;Ud[jj] != '|';jj ++);
	    	char *Uquantity = xmalloc(Ud+startpos,jj - startpos + 1);
        	int Uq1 = atoi(Uquantity);
			int Uq2 = atoi(Uquan);
			//get the extendedprice
			Ucount = 0;
	    	startpos = ++jj;
			for (;Ud[jj] != '|'; jj++);
			char *Uextend = xmalloc(Ud + startpos,jj - startpos + 1);
			if (Uq1 == Uq2){
				double Uextendedprice = atof(Uextend);
				Usum += Uextendedprice * atof(Uqdis);
			
			}
			status = page__seq(dbp3,&Ukey,&UData,R_NEXT,&e);
		}

		printf("Uquery done! the total is %.1lf\n",Usum);
		if (Usum > 0.0)
			fprintf(qout,"%.1lf\n",Usum);
		else
			fprintf(qout,"%s\n","NULL");
	}
	else{
        
		//范围查询---------------------------------------------------------
		char *Uupper1 = malloc(11*sizeof(char));
		char *Ulower1 = malloc(11*sizeof(char));
		char *Uupper2 = malloc(5*sizeof(char));
		char *Ulower2 = malloc(5*sizeof(char));
		char *Uquantity =malloc(4*sizeof(char));
		sscanf(buf_1,"%s %s %s %s %s",Ulower1,Uupper1,Ulower2,Uupper2,Uquantity);
	
		char *startKey = malloc(37*sizeof(char));
		char *endKey = malloc(37*sizeof(char));
		multi_str2bin(startKey,Ulower1,Ulower2);
		multi_str2bin(endKey,Uupper1,Uupper2);
	
		char *bin_up1 = malloc(29*sizeof(char));
		char *bin_low1 = malloc(29*sizeof(char));
		char *bin_up2 = malloc(9*sizeof(char));
		char *bin_low2 = malloc(9*sizeof(char));

		str2bin(Uupper1,bin_up1);
		str2bin(Uupper2,bin_up2);
		str2bin(Ulower1,bin_low1);
		str2bin(Ulower2,bin_low2);
	
		DBT UrKey,UrData;
		UrKey.data = startKey;
		UrKey.size = 37;
    	status = page__seq(dbp3,&UrKey,&UrData,R_CURSOR,&e);
		pgno_t tempNum = e.page->pgno;
   
   	    int Uq1 = atoi(Uquantity);
		double Rtotal = 0;
		int Rsum = 0;
		while (status == RET_SUCCESS){
			char *Ud = UrData.data; 
	    	pgno_t tempNum = e.page->pgno;
			if (strcmp((char*)UrKey.data,endKey) >= 0)
	    	{	
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
	    	char *Uquan = xmalloc(Ud+startpos,jj - startpos + 1);
			int Uq2 = atoi(Uquan);
		//get the extendedprice
			Ucount = 0;
	    	startpos = ++jj;
			for (;Ud[jj] != '|'; jj++);
			char *Uextend = xmalloc(Ud + startpos,jj - startpos + 1);
        
			char *dismen1 = malloc(29*sizeof(char));
			char *dismen2 = malloc(9*sizeof(char));
			char *Utemp = UrKey.data;
			bin2two(Utemp,dismen1,dismen2);
			if((Uq1 > Uq2) && strcmp(dismen1,bin_up1) < 0 && strcmp(dismen1,bin_low1) > 0 && strcmp(dismen2,bin_up2) < 0 && strcmp(dismen2,bin_low2) > 0){

				double Uextendedprice = atof(Uextend);
				Rtotal += Uextendedprice * atof(dis);
				Rsum ++;
			}
			DBT tKey = UrKey;
			status = page__seq(dbp3,&UrKey,&UrData,R_NEXT,&e);
		// 比较这时的key是否在范围内，不是则进行找nisp,是则继续往下查询
			Utemp = UrKey.data;
			bin2two(Utemp,dismen1,dismen2);
			if (strcmp(dismen1,bin_up1) <= 0 && strcmp(dismen1,bin_low1) >= 0 && strcmp(dismen2,bin_up2) <= 0 && strcmp(dismen2,bin_low2) >= 0 ){
				continue;
			}
			else{
			//开始找一下个开始点
				char *nisp = UrKey.data;
				getNextValue(nisp,bin_low1,bin_up1,bin_low2,bin_up2,UrKey);//找nisp的函数
		    	UrKey.data = nisp;
		    	UrKey.size = 37;
				status = page__seq(dbp3,&UrKey,&UrData,R_CURSOR,&e);
			
			}
        
	
		}

   		printf("UBtree range query done! the total is %.1lf   there are %d records\n",Rtotal,Rsum);
		if (Rtotal > 0.0)
			fprintf(qout,"%.1lf\n",Rtotal);
		else
			fprintf(qout,"%s\n","NULL");
	}

	}
   dbp3->close(dbp3);

   return 0;
}

void getNextValue(char *nisp,char *bin_low1,char *bin_up1,char *bin_low2,char *bin_up2,DBT UrKey)
{
     char *dismen1 = malloc(29*sizeof(char));
     char *dismen2 = malloc(9*sizeof(char));
     bin2two(nisp,dismen1,dismen2);
			int flag[3];
			int outstep[3];
			int savemin[3];
			int savemax[3];
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

			//-------------各维提取相关信息完毕-----------------------
			int haveFound = 0;
			position p;
			int tobeChange;
			if (outstep[1] > outstep[2]){
				if (dismen2[outstep[2]] == '0'){
					dismen2[outstep[2]] == '1';
					haveFound = 1;

				}
				p.dimension = 2;
				p.index = outstep[2];
			}
			else{
				if (dismen1[outstep[1]] == '0'){
					dismen1[outstep[1]] == '1';
					haveFound = 1;
				}
				p.dimension = 1;
				p.index = outstep[1];
			}
				
	
			if (!haveFound){
				int changebp = find_pos(p);
                int j = changebp;
				char *k = UrKey.data;
				for (;j >= 0;j --){
					if (k[j] == '0'){
						position p1 = where(j);
						if (p1.index >= savemax[p1.dimension]){
							tobeChange = j;
							break;
						}
					}
				}
			}
			else
				tobeChange = find_pos(p);
			
			nisp[tobeChange] = '1';
			position po = where(tobeChange);
			if (po.dimension == 1)
				dismen1[po.index] = '1';
			else
				dismen2[po.index] = '1';
            
			
			information(flag[1],&savemin[1],&savemax[1],&outstep[1],dismen1,bin_up1,bin_low1);
            information(flag[2],&savemin[2],&savemax[2],&outstep[2],dismen2,bin_up2,bin_low2);

			//找到把0变成1的点，下面开始各维度调整----------------------
			//第一维：
			if (flag[1] == -1)
			{	
				strcpy(dismen1,bin_low1);
				int ii;
				for (ii = 0;ii < strlen(dismen1);ii ++){
					if (ii < 8)
						nisp[ii * 2] = dismen1[ii];
					else
						nisp[ii + 8] = dismen1[ii];
				}
			}
			else{
				position BP;
				BP.dimension = 1;
				BP.index = savemin[1];
				int pos = find_pos(BP);
				position P = where(tobeChange);
				if (tobeChange >= pos){
					int start = 0;
					for (;start < strlen(dismen1) ;start ++){
						position poss;
						poss.dimension = 1;
						poss.index = start;
						int loca = find_pos(poss);
						if (loca > tobeChange){
							dismen1[start] = '0';
							if (start < 8)
								nisp[start*2] = '0';
							else
								nisp[start+8] = '0';
						}
					}

				}
				else{
					int start = 0;
					for (;start < strlen(dismen1); start ++){
						position poss;
						poss.dimension = 1;
						poss.index = start;
						int loca = find_pos(poss);
						if (loca > tobeChange){
							dismen1[start] = bin_low1[start];
							if (start < 8)
								nisp[start*2] = bin_low1[start];
							else
								nisp[start+8] = bin_low1[start];
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
						nisp[ii*2 + 1] = dismen2[ii];
					else
						nisp[ii + 8] = dismen2[ii];
				}
			}
			else{
				position BP;
				BP.dimension = 2;
				BP.index = savemin[2];
				int pos = find_pos(BP);
				position P = where(tobeChange);
				if (tobeChange >= pos){
					int start = 0;
					for (;start < strlen(dismen2) ;start ++){
						position poss;
						poss.dimension = 2;
						poss.index = start;
						int loca = find_pos(poss);
						if (loca > tobeChange){
							dismen2[start] = '0';
							if (start < 8)
								nisp[start*2 + 1] = '0';
							else
								nisp[start+8] = '0';
						}
					}
				}
				else{
					int start = 0;
					for (;start < strlen(dismen2); start ++){
						position poss;
						poss.dimension = 2;
						poss.index = start;
						int loca = find_pos(poss);
						if (loca > tobeChange){
							dismen2[start] = bin_low2[start];
							if (start < 8)
								nisp[start*2 + 1] = bin_low2[start];
							else
								nisp[start+8] = bin_low2[start];
						}
					}
				}
			}
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
