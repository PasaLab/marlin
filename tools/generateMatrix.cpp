#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

//usage: argv[1] is the rows of the matrix, argv[2] is the columns of the matrix
//after build this code, turn the out to the file to store matrix
int main(int argc, char* argv[])
{
	int row,column;
	sscanf(argv[1],"%d",&row);
	sscanf(argv[2],"%d",&column);
	//printf("%d,%d",row,column);
	srand(time(NULL));
	for(int  i =0; i < row; i++)
	{
		fprintf(stdout,"%d:",i);
		for(int j = 0; j < column; j++)
		{
			fprintf(stdout,"%f",((float)rand())/RAND_MAX*5);
			if(j < column - 1) fprintf(stdout,",");
		}
		fprintf(stdout,"\n");
	}
	return 0;
}
