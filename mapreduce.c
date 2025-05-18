#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <ctype.h>
#include <stdint.h>
#include <stdbool.h>

// Configuration constants
#define MAX_THREADS 16
#define MAX_KEY_LENGTH 128
#define MAX_VALUE_LENGTH 1024
#define INITIAL_BUCKET_SIZE 64
#define MAX_LINE_LENGTH 4096
#define PARTITIONS 16

// KeyValue structure for map and reduce operations
typedef struct {
    char key[MAX_KEY_LENGTH];
    char value[MAX_VALUE_LENGTH];
} KeyValue;

// List of KeyValue pairs
typedef struct {
    KeyValue* data;
    size_t size;
    size_t capacity;
} KeyValueList;

// Partition for intermediate results
typedef struct {
    KeyValueList kvs;
    pthread_mutex_t mutex;
} Partition;

// MapReduce context
typedef struct {
    // Input and output
    KeyValueList input;
    KeyValueList output;
    
    // Intermediate results
    Partition partitions[PARTITIONS];
    
    // Function pointers for map and reduce
    void (*map_function)(char* key, char* value, KeyValueList* output);
    void (*reduce_function)(char* key, KeyValueList* values, KeyValueList* output);
    
    // Threading control
    int num_threads;
    pthread_t threads[MAX_THREADS];
    
    // Synchronization
    pthread_mutex_t output_mutex;
    pthread_barrier_t phase_barrier;
} MapReduceContext;

// Forward declarations
KeyValueList create_kv_list();
void free_kv_list(KeyValueList* list);
void add_kv_pair(KeyValueList* list, const char* key, const char* value);
void init_mapreduce(MapReduceContext* ctx, int num_threads,
                    void (*map_fn)(char*, char*, KeyValueList*),
                    void (*reduce_fn)(char*, KeyValueList*, KeyValueList*));
void cleanup_mapreduce(MapReduceContext* ctx);
int run_mapreduce(MapReduceContext* ctx);
void* worker_thread(void* arg);
void run_map_phase(MapReduceContext* ctx, int thread_id);
void run_reduce_phase(MapReduceContext* ctx, int thread_id);
unsigned int hash_string(const char* str);
int compare_keys(const void* a, const void* b);
void sort_kv_list(KeyValueList* list);
void load_data_from_file(MapReduceContext* ctx, const char* filename);
void save_results_to_file(MapReduceContext* ctx, const char* filename);

// Initialize a new KeyValueList
KeyValueList create_kv_list() {
    KeyValueList list;
    list.capacity = INITIAL_BUCKET_SIZE;
    list.size = 0;
    list.data = (KeyValue*)malloc(list.capacity * sizeof(KeyValue));
    if (!list.data) {
        fprintf(stderr, "Failed to allocate memory for KeyValueList\n");
        exit(EXIT_FAILURE);
    }
    return list;
}

// Free a KeyValueList
void free_kv_list(KeyValueList* list) {
    if (list && list->data) {
        free(list->data);
        list->data = NULL;
        list->size = 0;
        list->capacity = 0;
    }
}

// Add a key-value pair to a KeyValueList
void add_kv_pair(KeyValueList* list, const char* key, const char* value) {
    if (!list || !key || !value) {
        return;
    }
    
    // Expand if needed
    if (list->size >= list->capacity) {
        list->capacity *= 2;
        KeyValue* new_data = (KeyValue*)realloc(list->data, list->capacity * sizeof(KeyValue));
        if (!new_data) {
            fprintf(stderr, "Failed to reallocate memory for KeyValueList\n");
            exit(EXIT_FAILURE);
        }
        list->data = new_data;
    }
    
    // Add the pair
    strncpy(list->data[list->size].key, key, MAX_KEY_LENGTH - 1);
    list->data[list->size].key[MAX_KEY_LENGTH - 1] = '\0';
    
    strncpy(list->data[list->size].value, value, MAX_VALUE_LENGTH - 1);
    list->data[list->size].value[MAX_VALUE_LENGTH - 1] = '\0';
    
    list->size++;
}

// Initialize the MapReduce context
void init_mapreduce(MapReduceContext* ctx, int num_threads,
                    void (*map_fn)(char*, char*, KeyValueList*),
                    void (*reduce_fn)(char*, KeyValueList*, KeyValueList*)) {
    if (!ctx || !map_fn || !reduce_fn) {
        fprintf(stderr, "Invalid MapReduce initialization\n");
        exit(EXIT_FAILURE);
    }
    
    // Set the number of threads
    ctx->num_threads = (num_threads > 0 && num_threads <= MAX_THREADS) ? 
                       num_threads : MAX_THREADS;
    
    // Set function pointers
    ctx->map_function = map_fn;
    ctx->reduce_function = reduce_fn;
    
    // Initialize input and output lists
    ctx->input = create_kv_list();
    ctx->output = create_kv_list();
    
    // Initialize partitions
    for (int i = 0; i < PARTITIONS; i++) {
        ctx->partitions[i].kvs = create_kv_list();
        pthread_mutex_init(&ctx->partitions[i].mutex, NULL);
    }
    
    // Initialize mutexes and barriers
    pthread_mutex_init(&ctx->output_mutex, NULL);
    pthread_barrier_init(&ctx->phase_barrier, NULL, ctx->num_threads + 1); // +1 for main thread
    
    printf("MapReduce initialized with %d threads\n", ctx->num_threads);
}

// Clean up the MapReduce context
void cleanup_mapreduce(MapReduceContext* ctx) {
    if (!ctx) {
        return;
    }
    
    // Free data structures
    free_kv_list(&ctx->input);
    free_kv_list(&ctx->output);
    
    // Free partitions
    for (int i = 0; i < PARTITIONS; i++) {
        free_kv_list(&ctx->partitions[i].kvs);
        pthread_mutex_destroy(&ctx->partitions[i].mutex);
    }
    
    // Destroy synchronization primitives
    pthread_mutex_destroy(&ctx->output_mutex);
    pthread_barrier_destroy(&ctx->phase_barrier);
    
    printf("MapReduce resources cleaned up\n");
}

// Run the MapReduce job
int run_mapreduce(MapReduceContext* ctx) {
    if (!ctx || ctx->input.size == 0) {
        fprintf(stderr, "No input data for MapReduce\n");
        return -1;
    }
    
    printf("Starting MapReduce job with %zu input items\n", ctx->input.size);
    
    // Create worker threads
    for (int i = 0; i < ctx->num_threads; i++) {
        int* thread_id = malloc(sizeof(int));
        if (!thread_id) {
            fprintf(stderr, "Failed to allocate thread ID\n");
            return -1;
        }
        *thread_id = i;
        
        if (pthread_create(&ctx->threads[i], NULL, worker_thread, (void*)thread_id) != 0) {
            fprintf(stderr, "Failed to create worker thread %d\n", i);
            free(thread_id);
            return -1;
        }
    }
    
    // Wait for all threads to be ready
    pthread_barrier_wait(&ctx->phase_barrier);
    printf("Map phase started\n");
    
    // Wait for Map phase to complete
    pthread_barrier_wait(&ctx->phase_barrier);
    printf("Map phase completed\n");
    
    // Sort the partitions (could be parallelized but kept simple)
    for (int i = 0; i < PARTITIONS; i++) {
        sort_kv_list(&ctx->partitions[i].kvs);
    }
    printf("Intermediate results sorted\n");
    
    // Start the Reduce phase
    pthread_barrier_wait(&ctx->phase_barrier);
    printf("Reduce phase started\n");
    
    // Wait for Reduce phase to complete
    pthread_barrier_wait(&ctx->phase_barrier);
    printf("Reduce phase completed\n");
    
    // Wait for all threads to finish
    for (int i = 0; i < ctx->num_threads; i++) {
        void* result;
        pthread_join(ctx->threads[i], &result);
        free(result); // Free the thread_id
    }
    
    printf("MapReduce job completed with %zu output items\n", ctx->output.size);
    return 0;
}

// Worker thread function
void* worker_thread(void* arg) {
    int thread_id = *(int*)arg;
    MapReduceContext* ctx = (MapReduceContext*)pthread_getspecific(0);
    if (!ctx) {
        // In a real implementation, we'd use thread-specific data
        // to store the context. For simplicity, we're accessing it directly
        // through a global variable or function parameter.
        ctx = (MapReduceContext*)arg;
    }
    
    // Signal that we're ready to start
    pthread_barrier_wait(&ctx->phase_barrier);
    
    // Run Map phase
    run_map_phase(ctx, thread_id);
    
    // Signal Map phase completion
    pthread_barrier_wait(&ctx->phase_barrier);
    
    // Wait for all maps to complete and sorting to finish
    pthread_barrier_wait(&ctx->phase_barrier);
    
    // Run Reduce phase
    run_reduce_phase(ctx, thread_id);
    
    // Signal Reduce phase completion
    pthread_barrier_wait(&ctx->phase_barrier);
    
    // Thread complete
    return arg; // Return thread_id for cleanup
}

// Run the Map phase for a thread
void run_map_phase(MapReduceContext* ctx, int thread_id) {
    // Each thread processes a portion of the input
    size_t items_per_thread = ctx->input.size / ctx->num_threads;
    size_t start_index = thread_id * items_per_thread;
    size_t end_index = (thread_id == ctx->num_threads - 1) ? 
                      ctx->input.size : (thread_id + 1) * items_per_thread;
    
    // Process assigned input items
    for (size_t i = start_index; i < end_index; i++) {
        // Create a temporary list for map output
        KeyValueList map_output = create_kv_list();
        
        // Call the map function
        ctx->map_function(ctx->input.data[i].key, ctx->input.data[i].value, &map_output);
        
        // Partition the map output
        for (size_t j = 0; j < map_output.size; j++) {
            // Determine the partition using a hash function
            unsigned int partition_index = hash_string(map_output.data[j].key) % PARTITIONS;
            
            // Add to the appropriate partition (thread-safe)
            pthread_mutex_lock(&ctx->partitions[partition_index].mutex);
            add_kv_pair(&ctx->partitions[partition_index].kvs, 
                        map_output.data[j].key, 
                        map_output.data[j].value);
            pthread_mutex_unlock(&ctx->partitions[partition_index].mutex);
        }
        
        // Clean up temporary map output
        free_kv_list(&map_output);
    }
}

// Run the Reduce phase for a thread
void run_reduce_phase(MapReduceContext* ctx, int thread_id) {
    // Each thread processes multiple partitions
    size_t partitions_per_thread = PARTITIONS / ctx->num_threads;
    size_t start_partition = thread_id * partitions_per_thread;
    size_t end_partition = (thread_id == ctx->num_threads - 1) ? 
                          PARTITIONS : (thread_id + 1) * partitions_per_thread;
    
    // Process assigned partitions
    for (size_t p = start_partition; p < end_partition; p++) {
        KeyValueList* partition = &ctx->partitions[p].kvs;
        
        // Process all keys in the partition
        for (size_t i = 0; i < partition->size; ) {
            // Get the current key
            char* current_key = partition->data[i].key;
            
            // Collect all values for this key
            KeyValueList values = create_kv_list();
            size_t j = i;
            
            // Group values with the same key
            while (j < partition->size && strcmp(partition->data[j].key, current_key) == 0) {
                add_kv_pair(&values, "", partition->data[j].value);
                j++;
            }
            
            // Create a temporary list for reduce output
            KeyValueList reduce_output = create_kv_list();
            
            // Call the reduce function
            ctx->reduce_function(current_key, &values, &reduce_output);
            
            // Add the reduce output to the final output (thread-safe)
            pthread_mutex_lock(&ctx->output_mutex);
            for (size_t k = 0; k < reduce_output.size; k++) {
                add_kv_pair(&ctx->output, reduce_output.data[k].key, reduce_output.data[k].value);
            }
            pthread_mutex_unlock(&ctx->output_mutex);
            
            // Clean up
            free_kv_list(&values);
            free_kv_list(&reduce_output);
            
            // Move to the next key
            i = j;
        }
    }
}

// Hash function for strings
unsigned int hash_string(const char* str) {
    unsigned int hash = 5381;
    int c;
    
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    
    return hash;
}

// Comparison function for sorting key-value pairs
int compare_keys(const void* a, const void* b) {
    KeyValue* kv1 = (KeyValue*)a;
    KeyValue* kv2 = (KeyValue*)b;
    return strcmp(kv1->key, kv2->key);
}

// Sort a KeyValueList by key
void sort_kv_list(KeyValueList* list) {
    if (!list || list->size == 0) {
        return;
    }
    
    qsort(list->data, list->size, sizeof(KeyValue), compare_keys);
}

// Load input data from a file
void load_data_from_file(MapReduceContext* ctx, const char* filename) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Failed to open input file: %s\n", filename);
        return;
    }
    
    char line[MAX_LINE_LENGTH];
    int line_number = 0;
    
    while (fgets(line, sizeof(line), file)) {
        // Remove trailing newline
        size_t len = strlen(line);
        if (len > 0 && line[len-1] == '\n') {
            line[len-1] = '\0';
        }
        
        // Create a key for the line (line number as string)
        char key[16];
        sprintf(key, "%d", line_number++);
        
        // Add to input list
        add_kv_pair(&ctx->input, key, line);
    }
    
    fclose(file);
    printf("Loaded %zu lines from %s\n", ctx->input.size, filename);
}

// Save results to a file
void save_results_to_file(MapReduceContext* ctx, const char* filename) {
    FILE* file = fopen(filename, "w");
    if (!file) {
        fprintf(stderr, "Failed to open output file: %s\n", filename);
        return;
    }
    
    for (size_t i = 0; i < ctx->output.size; i++) {
        fprintf(file, "%s\t%s\n", ctx->output.data[i].key, ctx->output.data[i].value);
    }
    
    fclose(file);
    printf("Saved %zu results to %s\n", ctx->output.size, filename);
}

//---------------------------------------------------------------
// Example: Word Count implementation
//---------------------------------------------------------------

// Map function for word count
void word_count_map(char* key, char* value, KeyValueList* output) {
    char* word = strtok(value, " \t\n\r\f,.:;?!\"'()[]{}");
    
    while (word != NULL) {
        // Convert word to lowercase
        char lowercase[MAX_KEY_LENGTH];
        size_t i = 0;
        while (word[i] && i < MAX_KEY_LENGTH - 1) {
            lowercase[i] = tolower(word[i]);
            i++;
        }
        lowercase[i] = '\0';
        
        // Skip empty words
        if (strlen(lowercase) > 0) {
            // Emit (word, "1")
            add_kv_pair(output, lowercase, "1");
        }
        
        word = strtok(NULL, " \t\n\r\f,.:;?!\"'()[]{}");
    }
}

// Reduce function for word count
void word_count_reduce(char* key, KeyValueList* values, KeyValueList* output) {
    // Sum up all the counts
    int count = 0;
    for (size_t i = 0; i < values->size; i++) {
        count += atoi(values->data[i].value);
    }
    
    // Convert count to string
    char count_str[32];
    sprintf(count_str, "%d", count);
    
    // Emit (word, count)
    add_kv_pair(output, key, count_str);
}

//---------------------------------------------------------------
// Example: Character Frequency implementation
//---------------------------------------------------------------

// Map function for character frequency
void char_freq_map(char* key, char* value, KeyValueList* output) {
    for (size_t i = 0; i < strlen(value); i++) {
        if (isalpha(value[i])) {
            // Convert to lowercase
            char c = tolower(value[i]);
            
            // Create a key for the character
            char char_key[2] = {c, '\0'};
            
            // Emit (char, "1")
            add_kv_pair(output, char_key, "1");
        }
    }
}

// Reduce function for character frequency (same as word count)
void char_freq_reduce(char* key, KeyValueList* values, KeyValueList* output) {
    // Sum up all the counts
    int count = 0;
    for (size_t i = 0; i < values->size; i++) {
        count += atoi(values->data[i].value);
    }
    
    // Convert count to string
    char count_str[32];
    sprintf(count_str, "%d", count);
    
    // Emit (char, count)
    add_kv_pair(output, key, count_str);
}

//---------------------------------------------------------------
// Main function
//---------------------------------------------------------------

int main(int argc, char* argv[]) {
    // Check arguments
    if (argc < 3) {
        printf("Usage: %s <input_file> <output_file> [word_count|char_freq]\n", argv[0]);
        return 1;
    }
    
    const char* input_file = argv[1];
    const char* output_file = argv[2];
    
    // Determine which MapReduce job to run
    bool run_word_count = true;  // Default
    if (argc >= 4 && strcmp(argv[3], "char_freq") == 0) {
        run_word_count = false;
    }
    
    // Initialize MapReduce context
    MapReduceContext ctx;
    if (run_word_count) {
        printf("Running Word Count MapReduce job\n");
        init_mapreduce(&ctx, 4, word_count_map, word_count_reduce);
    } else {
        printf("Running Character Frequency MapReduce job\n");
        init_mapreduce(&ctx, 4, char_freq_map, char_freq_reduce);
    }
    
    // Load input data
    load_data_from_file(&ctx, input_file);
    
    // Run MapReduce
    run_mapreduce(&ctx);
    
    // Sort the output for better readability
    sort_kv_list(&ctx.output);
    
    // Save results
    save_results_to_file(&ctx, output_file);
    
    // Display some results
    printf("\nTop results:\n");
    size_t display_count = ctx.output.size < 10 ? ctx.output.size : 10;
    for (size_t i = 0; i < display_count; i++) {
        printf("%s: %s\n", ctx.output.data[i].key, ctx.output.data[i].value);
    }
    
    // Clean up
    cleanup_mapreduce(&ctx);
    
    return 0;
}