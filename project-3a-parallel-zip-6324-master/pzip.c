#include <pthread.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h> //POSIX header file
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#define BUFFER (32)
int qsize = 32;
typedef struct {
	char *letters;
	size_t index;
	size_t size;
} file_strct;

typedef struct {
	char *final;
	size_t size;
} compressed;



int page_size;						
compressed *out_arr;				
file_strct *works;				
static int fillptr = 0;
static int useptr = 0;
static int numfull = 0;
static int chunks;				
volatile int flag;			

pthread_mutex_t lock  = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t fill = PTHREAD_COND_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER;


void do_put(file_strct chunk)
{
	works[fillptr] = chunk;
	fillptr = (fillptr + 1) % qsize;
	numfull++;
}

file_strct do_get()
{
	file_strct ret = works[useptr];
	useptr = (useptr + 1) % qsize;
	numfull--;
	return ret;
}


void rle(file_strct chunk, compressed *tmp)
{
	char* start = chunk.letters;
	size_t len = chunk.size;
	char end = *start;
	char *output = malloc(chunk.size * 8);
	char *curr = output;
	int count = 0;

	for (int i = 0; i < len; ++i)
	{
		if(start[i] != end)
		{
			*((int*)curr) = count;
			curr[4] = end;
			curr += 5;
			end = start[i];
			count = 1;
		}
		else count++;
	}

	*((int*)curr) = count;
	curr[4] = start[len-1];
	curr += 5;
	size_t fin_size = curr-output;

	char *res = malloc(fin_size);
	memcpy(res, output, curr-output);
	tmp->size = fin_size;
	tmp->final = res;

	free(output);
	munmap(chunk.letters, chunk.size);
	return;
    }



void *consumer(void *ptr)
{
	while (1)
	{
		file_strct task;
		pthread_mutex_lock(&lock);
		if (numfull == 0 && !flag)
		{
			pthread_mutex_unlock(&lock);
			pthread_exit(0);
		}
		while (numfull == 0 && flag)
		{
			pthread_cond_wait(&fill, &lock);
		}
		task = do_get();
		pthread_cond_signal(&empty);
		pthread_mutex_unlock(&lock);
		compressed *tmp = &out_arr[task.index];
		rle(task, tmp);
	}
	pthread_exit(0);
}


int main(int argc, char **argv)
{
	if (argc <= 1)
	{
		printf("pzip: file1 [file2 ...]\n");
		exit(1);
	}

	page_size = getpagesize()*8;
	int nprocs = get_nprocs();
	int numfiles = argc;
	flag = 1;
	chunks = 0;

	for (int i = 1; i < numfiles; i++)
	{
		int fd = open(argv[i], O_RDONLY);

		struct stat statbuf;
		fstat(fd, &statbuf);
		double file_size = (double) statbuf.st_size;
		int chunkcount = file_size / ((double)page_size);
		if ( ((size_t) file_size) % page_size != 0)
			chunkcount++;
		if (file_size != 0)
			chunks += chunkcount;
		close(fd);
	}

	works = malloc(sizeof(file_strct) * qsize);
	out_arr = malloc(sizeof(compressed) * chunks);

	int count = 0;
	int fil = 0;
	int next = 1;
	int size_left = 0;
	int offset = 0;
	int chunk_size = 0;
	void *map = NULL;

	pthread_t threads[nprocs];
	for (int i = 0; i < nprocs; i++)
	{
		pthread_create(&threads[i], NULL, consumer, NULL);
	}

	for (int file = 1; file < numfiles;)
	{
		if (next)
		{
			fil = open(argv[file], O_RDONLY);

			struct stat statbuf;
			fstat(fil, &statbuf);
			size_left = (size_t) statbuf.st_size;
			offset = 0;
			next = 0;
		}

		chunk_size = size_left > page_size ? page_size : size_left;

		if (chunk_size == 0)
		{
			file++;
			close(fil);
			next = 1;
			continue;
		}

		map = mmap(NULL, chunk_size, PROT_READ , MAP_PRIVATE, fil, offset);
		file_strct fi;
		fi.letters = map;
		fi.size  = chunk_size;
		fi.index = count;

		pthread_mutex_lock(&lock);
		while (numfull == qsize)
		pthread_cond_wait(&empty, &lock);
		do_put(fi);
		pthread_cond_signal(&fill);
		pthread_mutex_unlock(&lock);
		size_left -= chunk_size;
		offset  += chunk_size;

		if (size_left <= 0)
		{
			file++;
			close(fil);
			next = 1;
		}

		count++;
	}
	flag = 0;

	pthread_cond_broadcast(&fill);
	for (int i = 0; i < nprocs; i++)
	{
		pthread_join(threads[i], NULL);
	}


	char *end = NULL;

	for (int i = 0; i < chunks; i++)
	{
		char *bin = out_arr[i].final;
		int n = out_arr[i].size;

		if (end && end[4] == bin[4])
		{
			*((int*)bin) += *((int*)end);
		}
		else if(end)
		{
			fwrite(end, 5, 1, stdout);
		}
		fwrite(bin, n - 5, 1, stdout);
		end = bin + n - 5;
	}

	fwrite(end, 5, 1, stdout);

	return 0;
}
