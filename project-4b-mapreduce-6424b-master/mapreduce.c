#include "mapreduce.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#define NUM_MAPS 999


// Define hashtable

typedef struct value_node{
    char* value;
    struct value_node* next;
}v_node;

typedef struct key_node{
    char* key;
    v_node* head;
    struct key_node* next;
}k_node;

typedef struct key_entry {
    k_node* head;
    pthread_mutex_t lock;
}k_entry;

typedef struct h_table_entry{
    k_entry map[NUM_MAPS];
    int key_count;
    pthread_mutex_t lock;
    k_node* sorted;
    int curr_visit;
}hash_table;

int counter;
pthread_mutex_t fileLock;
int NUM_REDUCER;
int NUM_FILES;
char** File_Names;
Partitioner partitioner;
Mapper mapper;
Reducer reducer;
hash_table h_table[64];

void
init(int argc, char* argv[], Mapper mapper_arg, int num_reducers, Partitioner partition_arg, Reducer reducer_arg) {
    int rc = pthread_mutex_init(&fileLock, NULL);
    assert(rc == 0);

    NUM_FILES = argc - 1;
    File_Names = (argv + 1);
    mapper = mapper_arg;
    reducer = reducer_arg;
    partitioner = partition_arg;
    
    counter = 0;
    NUM_REDUCER = num_reducers;

    //hash initialization
    int i;
    for (i = 0;i < NUM_REDUCER; i++){
        pthread_mutex_init(&h_table[i].lock, NULL);
        h_table[i].key_count = 0;
        h_table[i].curr_visit = 0;
        h_table[i].sorted = NULL;

	for (int j = 0; j < NUM_MAPS; j++){
            h_table[i].map[j].head = NULL;
            pthread_mutex_init(&h_table[i].map[j].lock, NULL);
        } 
    }

}
char*
get_func(char *key, int partition_number){
    k_node* sorted_arr = h_table[partition_number].sorted;
    char* value;
    for(;;){
      int index = h_table[partition_number].curr_visit;
      if (strcmp(sorted_arr[index].key, key) == 0){
	if (sorted_arr[index].head == NULL)
	  return NULL;
	v_node* temp = sorted_arr[index].head->next;
	value = sorted_arr[index].head->value;
	sorted_arr[index].head = temp;
	return value;
      }
      else
	{
	  h_table[partition_number].curr_visit++;
	  continue;
	}
      return NULL;
    }
}
void*
Map_Worker(void* arg){
  for (;;){
    char* filename;
    pthread_mutex_lock(&fileLock);
    if(counter >= NUM_FILES){
      pthread_mutex_unlock(&fileLock);
      return NULL;
    }
    filename = File_Names[counter++];
    pthread_mutex_unlock(&fileLock);
    (*mapper)(filename);
  }
}

int
compare_func(const void* a, const void* b){
    char* a1 = ((k_node*)a)->key;
    char* a2 = ((k_node*)b)->key;
    return strcmp(a1, a2);
    
}

// Table map remove removes the original mappings of keys&value
// while the sorted version is the only one that gets stored.
void
table_map_remove (int partition_number ){
  for (int k = 0; k < NUM_MAPS; k++) {
    k_node *ktemp = h_table[partition_number].map[k].head;
    if (ktemp == NULL)
      continue;
    while (ktemp != NULL) {
      free(ktemp->key);
      ktemp->key = NULL;
      v_node *vtemp = ktemp->head;
      while (vtemp != NULL) {
        free(vtemp->value);
        vtemp->value = NULL;
        v_node* vtemp_nxt = vtemp->next;
        free(vtemp);
        vtemp = vtemp_nxt;
      }
      vtemp  = NULL;
      k_node* ktemp_nxt = ktemp->next;
      free(ktemp);
      ktemp = ktemp_nxt;
    }  
    ktemp = NULL;
  }
  free(h_table[partition_number].sorted);
  h_table[partition_number].sorted = NULL; 
}
/*
void table_map_append_key(unsigned long h_number,v_node* new_v, char* key, char* value){
  
  unsigned long m_number = MR_DefaultHashPartition(key, NUM_MAPS);
  new_v->value = malloc(sizeof(char)*20);
  strcpy(new_v->value, value);
  new_v->next = NULL;
  k_node *new_key = malloc(sizeof(k_node));
  if (new_key == NULL) {
    perror("malloc");
    pthread_mutex_unlock(&h_table[h_number].map[m_number].lock);
    return; // fail                                                                                       
  }
  new_key->head = new_v;
  new_key->next = h_table[h_number].map[m_number].head;
  h_table[h_number].map[h_number].head = new_key;
  
  new_key->key = malloc(sizeof(char)*20);
  if (new_key->key == NULL){
    printf("ERROR MALLOC FOR VALUE");
  }
  strcpy(new_key->key, key);
  
  pthread_mutex_lock(&h_table[m_number].lock);
  h_table[h_number].key_count++;
  pthread_mutex_unlock(&h_table[m_number].lock);
  
  }*/

void*
Reduce_Worker(void* args){
    int partition_number = *(int*)args;
    free(args); 
    args = NULL;
    
    if(h_table[partition_number].key_count == 0){
        return NULL;
    }

    int k_count = h_table[partition_number].key_count;
    int count = 0;
    h_table[partition_number].sorted = malloc(sizeof(k_node)*k_count);
    for (int i = 0; i < NUM_MAPS; i++){
        k_node *curr = h_table[partition_number].map[i].head;
        if (curr == NULL)
            continue;
        while (curr != NULL){
            h_table[partition_number].sorted[count] = *curr;
            count++;
            curr = curr -> next;
        }
    }    
    // Qsort (array, no. of entry, size of entry, compare function)
    qsort(h_table[partition_number].sorted, h_table[partition_number].key_count, sizeof(k_node), compare_func);
    
    for (int i = 0; i < h_table[partition_number].key_count; i++){
        char *key = h_table[partition_number].sorted[i].key;
	// Call on reduce function in at user side , and get_func only goes through sorted array
	(*reducer)(key,get_func,partition_number);
    }

    // Cleaning the unsorted array when a partition task finished
    table_map_remove(partition_number);
    // Where segfault occurred 
    return NULL;
}


void
MR_Emit(char *key, char *value){
  unsigned long partition_num;
  if(partitioner != NULL)
    {
      partition_num = (*partitioner)(key, NUM_REDUCER);
    }
  else
    {
      partition_num = MR_DefaultHashPartition(key,NUM_REDUCER);
    }
  
  unsigned long map_num = MR_DefaultHashPartition(key,NUM_MAPS);
  pthread_mutex_lock(&h_table[partition_num].map[map_num].lock);
  k_node *tmp = h_table[partition_num].map[map_num].head;
  while (tmp != NULL) {
    if (strcmp(tmp->key, key) == 0) 
      break;
    tmp = tmp->next;
  }    
 
  v_node *new_val = malloc(sizeof(v_node));
  if (new_val == NULL) {
    pthread_mutex_unlock(&h_table[partition_num].map[map_num].lock);
    return;
  }

  new_val->value = malloc(sizeof(char) * 20);
  strcpy(new_val->value, value);
  new_val->next = NULL;
  
  if (tmp == NULL)
    {
      // No exisitng keys
      k_node *n_knode = malloc(sizeof(k_node));
      if (n_knode == NULL) {
	pthread_mutex_unlock(&h_table[partition_num].map[map_num].lock);
	return;
      }
      n_knode->head = new_val;
      n_knode->next = h_table[partition_num].map[map_num].head;
      h_table[partition_num].map[map_num].head = n_knode;
      
      n_knode->key = malloc(sizeof(char) * 20);
      strcpy(n_knode->key, key);
      pthread_mutex_lock(&h_table[partition_num].lock);
      h_table[partition_num].key_count++;
      pthread_mutex_unlock(&h_table[partition_num].lock);
    }
  else
    {
      new_val->next = tmp->head;
      tmp->head = new_val;
    }
  
  pthread_mutex_unlock(&h_table[partition_num].map[map_num].lock);
}


unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}



void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers, 
        Reducer reduce, int num_reducers, 
        Partitioner partition){

    init(argc, argv, map, num_reducers, partition, reduce);

    // create map threads
    pthread_t map_threads[num_mappers];
    for (int i = 0; i < num_mappers; i++){
        pthread_create(&map_threads[i], NULL, Map_Worker, NULL);
    }
    for (int j = 0; j < num_mappers; j++){
        pthread_join(map_threads[j], NULL);
    }

    
    // create reduce threads
    pthread_t reduce_threads[num_reducers];
    for (int k = 0; k < num_reducers; k++){
        void* arg = malloc(4);
        *(int*)arg = k;
        pthread_create(&reduce_threads[k], NULL, Reduce_Worker, arg);
    }
    for (int l = 0; l < num_reducers; l++){
        pthread_join(reduce_threads[l], NULL);
    }
}
