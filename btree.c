#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

// Define the order of the B-tree
// For a B-tree of order m:
// - Each node can have at most m children and m-1 keys
// - Each node (except root) must have at least ceil(m/2) children
// - The root must have at least 2 children unless it's a leaf
#define ORDER 5

// Value type for the B-tree (can be changed to any data type)
typedef struct {
    void* data;          // Pointer to actual data
    size_t data_size;    // Size of the data
} Value;

// Key-Value pair for the B-tree
typedef struct {
    int key;             // Integer key
    Value value;         // Value associated with the key
} KeyValue;

// B-tree node structure
typedef struct BTreeNode {
    bool is_leaf;                    // Whether this node is a leaf
    int num_keys;                    // Number of keys currently in the node
    KeyValue keys[ORDER - 1];        // Array of key-value pairs
    struct BTreeNode* children[ORDER]; // Array of child pointers
} BTreeNode;

// B-tree structure
typedef struct {
    BTreeNode* root;     // Root node of the B-tree
    int min_degree;      // Minimum degree (minimum number of keys in a node is min_degree-1)
} BTree;

// Forward declarations
BTree* btree_create();
void btree_destroy(BTree* tree);
void btree_destroy_node(BTreeNode* node);
BTreeNode* btree_create_node(bool is_leaf);
Value* btree_search(BTree* tree, int key);
Value* btree_search_node(BTreeNode* node, int key);
void btree_insert(BTree* tree, int key, void* data, size_t data_size);
void btree_split_child(BTreeNode* parent, int index);
void btree_insert_non_full(BTreeNode* node, int key, void* data, size_t data_size);
bool btree_delete(BTree* tree, int key);
bool btree_delete_from_node(BTreeNode* node, int key);
void btree_remove_from_leaf(BTreeNode* node, int index);
void btree_remove_from_non_leaf(BTreeNode* node, int index);
int btree_get_predecessor(BTreeNode* node, int index);
int btree_get_successor(BTreeNode* node, int index);
void btree_fill_child(BTreeNode* node, int index);
void btree_borrow_from_prev(BTreeNode* node, int index);
void btree_borrow_from_next(BTreeNode* node, int index);
void btree_merge_children(BTreeNode* node, int index);
void btree_traverse(BTree* tree);
void btree_traverse_node(BTreeNode* node, int level);
void btree_print(BTree* tree);
void btree_print_node(BTreeNode* node, int level);

// Create a new B-tree
BTree* btree_create() {
    BTree* tree = (BTree*)malloc(sizeof(BTree));
    if (tree == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    
    tree->min_degree = ORDER / 2 + (ORDER % 2 == 0 ? 0 : 1); // Ceiling of ORDER/2
    tree->root = NULL;
    
    return tree;
}

// Free all memory used by the B-tree
void btree_destroy(BTree* tree) {
    if (tree == NULL) return;
    
    if (tree->root != NULL) {
        btree_destroy_node(tree->root);
    }
    
    free(tree);
}

// Recursively free a node and all its children
void btree_destroy_node(BTreeNode* node) {
    if (node == NULL) return;
    
    // Free all children first
    if (!node->is_leaf) {
        for (int i = 0; i <= node->num_keys; i++) {
            btree_destroy_node(node->children[i]);
        }
    }
    
    // Free the data in the node
    for (int i = 0; i < node->num_keys; i++) {
        free(node->keys[i].value.data);
    }
    
    // Free the node itself
    free(node);
}

// Create a new B-tree node
BTreeNode* btree_create_node(bool is_leaf) {
    BTreeNode* node = (BTreeNode*)malloc(sizeof(BTreeNode));
    if (node == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    
    node->is_leaf = is_leaf;
    node->num_keys = 0;
    
    // Initialize all children to NULL
    for (int i = 0; i < ORDER; i++) {
        node->children[i] = NULL;
    }
    
    return node;
}

// Search for a key in the B-tree
Value* btree_search(BTree* tree, int key) {
    if (tree == NULL || tree->root == NULL) {
        return NULL;
    }
    
    return btree_search_node(tree->root, key);
}

// Search for a key in a B-tree node
Value* btree_search_node(BTreeNode* node, int key) {
    // Find the first key greater than or equal to the search key
    int i = 0;
    while (i < node->num_keys && key > node->keys[i].key) {
        i++;
    }
    
    // If the key was found, return its value
    if (i < node->num_keys && key == node->keys[i].key) {
        return &node->keys[i].value;
    }
    
    // If this is a leaf node and key wasn't found, it's not in the tree
    if (node->is_leaf) {
        return NULL;
    }
    
    // Otherwise, search the appropriate child
    return btree_search_node(node->children[i], key);
}

// Insert a key-value pair into the B-tree
void btree_insert(BTree* tree, int key, void* data, size_t data_size) {
    if (tree == NULL) return;
    
    // If the tree is empty, create a root node
    if (tree->root == NULL) {
        tree->root = btree_create_node(true);
        tree->root->keys[0].key = key;
        
        // Create a copy of the data
        tree->root->keys[0].value.data = malloc(data_size);
        if (tree->root->keys[0].value.data == NULL) {
            fprintf(stderr, "Memory allocation failed\n");
            exit(EXIT_FAILURE);
        }
        
        memcpy(tree->root->keys[0].value.data, data, data_size);
        tree->root->keys[0].value.data_size = data_size;
        tree->root->num_keys = 1;
        return;
    }
    
    // If the root is full, the tree grows in height
    if (tree->root->num_keys == ORDER - 1) {
        // Create a new root
        BTreeNode* new_root = btree_create_node(false);
        
        // Make old root the first child of new root
        new_root->children[0] = tree->root;
        
        // Split the old root and move one key to the new root
        btree_split_child(new_root, 0);
        
        // New root has two children now. Decide which child gets the new key
        int i = 0;
        if (new_root->keys[0].key < key) {
            i++;
        }
        
        btree_insert_non_full(new_root->children[i], key, data, data_size);
        
        // Update the root
        tree->root = new_root;
    } else {
        // Root is not full, insert the key
        btree_insert_non_full(tree->root, key, data, data_size);
    }
}

// Split a child node
void btree_split_child(BTreeNode* parent, int index) {
    BTreeNode* child = parent->children[index];
    BTreeNode* new_child = btree_create_node(child->is_leaf);
    
    // The new child will have min_degree-1 keys (the right half of child)
    new_child->num_keys = ORDER / 2 - 1;
    
    // Copy the second half of keys from child to new_child
    for (int j = 0; j < new_child->num_keys; j++) {
        new_child->keys[j] = child->keys[j + ORDER / 2];
        
        // Clear the original pointers
        child->keys[j + ORDER / 2].value.data = NULL;
    }
    
    // If child is not a leaf, copy the second half of children as well
    if (!child->is_leaf) {
        for (int j = 0; j < ORDER / 2; j++) {
            new_child->children[j] = child->children[j + ORDER / 2];
            child->children[j + ORDER / 2] = NULL;
        }
    }
    
    // Reduce the number of keys in child
    child->num_keys = ORDER / 2 - 1;
    
    // Create space for the new child in the parent
    for (int j = parent->num_keys; j >= index + 1; j--) {
        parent->children[j + 1] = parent->children[j];
    }
    
    // Link the new child to the parent
    parent->children[index + 1] = new_child;
    
    // Move a key from child to parent
    for (int j = parent->num_keys - 1; j >= index; j--) {
        parent->keys[j + 1] = parent->keys[j];
    }
    
    parent->keys[index] = child->keys[ORDER / 2 - 1];
    child->keys[ORDER / 2 - 1].value.data = NULL; // Clear original pointer
    
    // Increment the number of keys in parent
    parent->num_keys++;
}

// Insert a key into a non-full node
void btree_insert_non_full(BTreeNode* node, int key, void* data, size_t data_size) {
    // Start from the rightmost element
    int i = node->num_keys - 1;
    
    if (node->is_leaf) {
        // Find the location to insert the new key and move all greater keys one space ahead
        while (i >= 0 && key < node->keys[i].key) {
            node->keys[i + 1] = node->keys[i];
            i--;
        }
        
        // Insert the new key
        node->keys[i + 1].key = key;
        
        // Create a copy of the data
        node->keys[i + 1].value.data = malloc(data_size);
        if (node->keys[i + 1].value.data == NULL) {
            fprintf(stderr, "Memory allocation failed\n");
            exit(EXIT_FAILURE);
        }
        
        memcpy(node->keys[i + 1].value.data, data, data_size);
        node->keys[i + 1].value.data_size = data_size;
        
        node->num_keys++;
    } else {
        // Find the child which is going to have the new key
        while (i >= 0 && key < node->keys[i].key) {
            i--;
        }
        i++; // Move to the right child
        
        // If the child is full, split it
        if (node->children[i]->num_keys == ORDER - 1) {
            btree_split_child(node, i);
            
            // After split, the middle key of children[i] goes up and children[i] is split
            // into two. See which of the two is going to have the new key
            if (key > node->keys[i].key) {
                i++;
            }
        }
        
        btree_insert_non_full(node->children[i], key, data, data_size);
    }
}

// Delete a key from the B-tree
bool btree_delete(BTree* tree, int key) {
    if (tree == NULL || tree->root == NULL) {
        return false;
    }
    
    bool result = btree_delete_from_node(tree->root, key);
    
    // If the root node has 0 keys, make its first child the new root
    if (tree->root->num_keys == 0) {
        BTreeNode* tmp = tree->root;
        
        if (tree->root->is_leaf) {
            tree->root = NULL;
        } else {
            tree->root = tree->root->children[0];
        }
        
        free(tmp);
    }
    
    return result;
}

// Delete a key from a node
bool btree_delete_from_node(BTreeNode* node, int key) {
    int idx = 0;
    
    // Find the index of the key, if it exists in this node
    while (idx < node->num_keys && node->keys[idx].key < key) {
        idx++;
    }
    
    // If the key is found in this node
    if (idx < node->num_keys && node->keys[idx].key == key) {
        if (node->is_leaf) {
            // Case 1: If the node is a leaf, simply remove the key
            btree_remove_from_leaf(node, idx);
        } else {
            // Case 2: If the node is not a leaf
            btree_remove_from_non_leaf(node, idx);
        }
        return true;
    }
    
    // If the key is not in this node
    
    // If this is a leaf node, the key is not in the tree
    if (node->is_leaf) {
        return false;
    }
    
    // Flag to indicate whether the key is in the last child of this node
    bool flag = (idx == node->num_keys);
    
    // If the child has less than min_degree keys, fill it
    if (node->children[idx]->num_keys < ORDER / 2) {
        btree_fill_child(node, idx);
    }
    
    // If the last child has been merged, recurse on the (idx-1)th child
    // Otherwise, recurse on the (idx)th child which now has at least min_degree keys
    if (flag && idx > node->num_keys) {
        return btree_delete_from_node(node->children[idx - 1], key);
    } else {
        return btree_delete_from_node(node->children[idx], key);
    }
}

// Remove a key from a leaf node
void btree_remove_from_leaf(BTreeNode* node, int idx) {
    // Free the data
    free(node->keys[idx].value.data);
    
    // Shift all keys after idx one place backward
    for (int i = idx + 1; i < node->num_keys; i++) {
        node->keys[i - 1] = node->keys[i];
    }
    
    // Reduce the number of keys
    node->num_keys--;
}

// Remove a key from a non-leaf node
void btree_remove_from_non_leaf(BTreeNode* node, int idx) {
    int key = node->keys[idx].key;
    
    // Case 2a: If the child that precedes key has at least min_degree keys,
    // find the predecessor of key in the subtree rooted at the child.
    // Replace key with its predecessor and recursively delete the predecessor.
    if (node->children[idx]->num_keys >= ORDER / 2) {
        int pred = btree_get_predecessor(node, idx);
        
        // Free the current data
        free(node->keys[idx].value.data);
        
        // Copy the predecessor's key and value
        node->keys[idx].key = pred;
        node->keys[idx].value = node->children[idx]->keys[node->children[idx]->num_keys - 1].value;
        
        // Clear the original value pointer
        node->children[idx]->keys[node->children[idx]->num_keys - 1].value.data = NULL;
        
        // Recursively delete the predecessor
        btree_delete_from_node(node->children[idx], pred);
    }
    
    // Case 2b: If the child that follows key has at least min_degree keys,
    // find the successor of key in the subtree rooted at the child.
    // Replace key with its successor and recursively delete the successor.
    else if (node->children[idx + 1]->num_keys >= ORDER / 2) {
        int succ = btree_get_successor(node, idx);
        
        // Free the current data
        free(node->keys[idx].value.data);
        
        // Copy the successor's key and value
        node->keys[idx].key = succ;
        node->keys[idx].value = node->children[idx + 1]->keys[0].value;
        
        // Clear the original value pointer
        node->children[idx + 1]->keys[0].value.data = NULL;
        
        // Recursively delete the successor
        btree_delete_from_node(node->children[idx + 1], succ);
    }
    
    // Case 2c: If both children have less than min_degree keys,
    // merge key and the right child into the left child,
    // then recursively delete the key from the merged child.
    else {
        btree_merge_children(node, idx);
        btree_delete_from_node(node->children[idx], key);
    }
}

// Get the predecessor of the key at index idx in node
int btree_get_predecessor(BTreeNode* node, int idx) {
    // Keep moving to the rightmost node until we reach a leaf
    BTreeNode* cur = node->children[idx];
    while (!cur->is_leaf) {
        cur = cur->children[cur->num_keys];
    }
    
    // Return the last key of the leaf
    return cur->keys[cur->num_keys - 1].key;
}

// Get the successor of the key at index idx in node
int btree_get_successor(BTreeNode* node, int idx) {
    // Keep moving to the leftmost node until we reach a leaf
    BTreeNode* cur = node->children[idx + 1];
    while (!cur->is_leaf) {
        cur = cur->children[0];
    }
    
    // Return the first key of the leaf
    return cur->keys[0].key;
}

// Fill the child at index idx which has less than min_degree keys
void btree_fill_child(BTreeNode* node, int idx) {
    // If the previous child has at least min_degree keys, borrow from it
    if (idx != 0 && node->children[idx - 1]->num_keys >= ORDER / 2) {
        btree_borrow_from_prev(node, idx);
    }
    // If the next child has at least min_degree keys, borrow from it
    else if (idx != node->num_keys && node->children[idx + 1]->num_keys >= ORDER / 2) {
        btree_borrow_from_next(node, idx);
    }
    // If neither sibling has enough keys, merge with a sibling
    else {
        // If idx is the last child, merge it with its previous sibling
        if (idx == node->num_keys) {
            btree_merge_children(node, idx - 1);
        }
        // Otherwise, merge it with its next sibling
        else {
            btree_merge_children(node, idx);
        }
    }
}

// Borrow a key from the previous child
void btree_borrow_from_prev(BTreeNode* node, int idx) {
    BTreeNode* child = node->children[idx];
    BTreeNode* sibling = node->children[idx - 1];
    
    // Shift all keys in child one step forward
    for (int i = child->num_keys - 1; i >= 0; i--) {
        child->keys[i + 1] = child->keys[i];
    }
    
    // If child is not a leaf, shift all its children one step forward
    if (!child->is_leaf) {
        for (int i = child->num_keys; i >= 0; i--) {
            child->children[i + 1] = child->children[i];
        }
    }
    
    // Set the first key of child equal to the key from parent
    child->keys[0] = node->keys[idx - 1];
    
    // If child is not a leaf, take the last child of sibling as the first child of child
    if (!child->is_leaf) {
        child->children[0] = sibling->children[sibling->num_keys];
        sibling->children[sibling->num_keys] = NULL;
    }
    
    // Move the last key from sibling to parent
    node->keys[idx - 1] = sibling->keys[sibling->num_keys - 1];
    
    // Clear the original value pointer in sibling
    sibling->keys[sibling->num_keys - 1].value.data = NULL;
    
    // Update key counts
    child->num_keys++;
    sibling->num_keys--;
}

// Borrow a key from the next child
void btree_borrow_from_next(BTreeNode* node, int idx) {
    BTreeNode* child = node->children[idx];
    BTreeNode* sibling = node->children[idx + 1];
    
    // The key from parent goes to child
    child->keys[child->num_keys] = node->keys[idx];
    
    // If child is not a leaf, the first child of sibling becomes the last child of child
    if (!child->is_leaf) {
        child->children[child->num_keys + 1] = sibling->children[0];
        sibling->children[0] = NULL;
    }
    
    // The first key from sibling goes to parent
    node->keys[idx] = sibling->keys[0];
    
    // Clear the original value pointer in sibling
    sibling->keys[0].value.data = NULL;
    
    // Shift all keys in sibling one step backward
    for (int i = 1; i < sibling->num_keys; i++) {
        sibling->keys[i - 1] = sibling->keys[i];
    }
    
    // If sibling is not a leaf, shift all its children one step backward
    if (!sibling->is_leaf) {
        for (int i = 1; i <= sibling->num_keys; i++) {
            sibling->children[i - 1] = sibling->children[i];
        }
        sibling->children[sibling->num_keys] = NULL;
    }
    
    // Update key counts
    child->num_keys++;
    sibling->num_keys--;
}

// Merge child[idx] with child[idx+1]
void btree_merge_children(BTreeNode* node, int idx) {
    BTreeNode* child = node->children[idx];
    BTreeNode* sibling = node->children[idx + 1];
    
    // Copy the key from parent to child
    child->keys[child->num_keys] = node->keys[idx];
    
    // Copy all keys from sibling to child
    for (int i = 0; i < sibling->num_keys; i++) {
        child->keys[i + child->num_keys + 1] = sibling->keys[i];
        sibling->keys[i].value.data = NULL; // Clear original pointer
    }
    
    // Copy all children from sibling to child
    if (!child->is_leaf) {
        for (int i = 0; i <= sibling->num_keys; i++) {
            child->children[i + child->num_keys + 1] = sibling->children[i];
            sibling->children[i] = NULL; // Clear original pointer
        }
    }
    
    // Shift all keys after idx in the parent one step backward
    for (int i = idx + 1; i < node->num_keys; i++) {
        node->keys[i - 1] = node->keys[i];
    }
    
    // Shift all children after idx+1 in the parent one step backward
    for (int i = idx + 2; i <= node->num_keys; i++) {
        node->children[i - 1] = node->children[i];
    }
    
    // Update key counts
    child->num_keys += sibling->num_keys + 1;
    node->num_keys--;
    
    // Free the memory for sibling
    free(sibling);
}

// Traverse the B-tree in-order
void btree_traverse(BTree* tree) {
    if (tree == NULL || tree->root == NULL) {
        printf("Empty tree\n");
        return;
    }
    
    printf("B-tree traversal:\n");
    btree_traverse_node(tree->root, 0);
    printf("\n");
}

// Traverse a node in-order
void btree_traverse_node(BTreeNode* node, int level) {
    if (node == NULL) return;
    
    int i;
    for (i = 0; i < node->num_keys; i++) {
        // Traverse the left child
        if (!node->is_leaf) {
            btree_traverse_node(node->children[i], level + 1);
        }
        
        // Print the key
        printf("Level %d: Key=%d\n", level, node->keys[i].key);
    }
    
    // Traverse the rightmost child
    if (!node->is_leaf) {
        btree_traverse_node(node->children[i], level + 1);
    }
}

// Print the B-tree structure
void btree_print(BTree* tree) {
    if (tree == NULL || tree->root == NULL) {
        printf("Empty tree\n");
        return;
    }
    
    printf("B-tree structure:\n");
    btree_print_node(tree->root, 0);
    printf("\n");
}

// Print a node and its children recursively
void btree_print_node(BTreeNode* node, int level) {
    if (node == NULL) return;
    
    // Print indentation
    for (int i = 0; i < level; i++) {
        printf("  ");
    }
    
    // Print keys in this node
    printf("Node (level %d): [", level);
    for (int i = 0; i < node->num_keys; i++) {
        printf("%d", node->keys[i].key);
        if (i < node->num_keys - 1) {
            printf(", ");
        }
    }
    printf("] %s\n", node->is_leaf ? "(Leaf)" : "");
    
    // Print children recursively
    if (!node->is_leaf) {
        for (int i = 0; i <= node->num_keys; i++) {
            btree_print_node(node->children[i], level + 1);
        }
    }
}

//---------------------------------------------------------------
// B-tree Index Manager Implementation
//---------------------------------------------------------------

// IndexRecord structure
typedef struct {
    int id;
    char name[50];
    int age;
} IndexRecord;

// Function to create an IndexRecord
IndexRecord* create_record(int id, const char* name, int age) {
    IndexRecord* record = (IndexRecord*)malloc(sizeof(IndexRecord));
    if (record == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    
    record->id = id;
    strncpy(record->name, name, sizeof(record->name) - 1);
    record->name[sizeof(record->name) - 1] = '\0';
    record->age = age;
    
    return record;
}

// Simple helper to print a record
void print_record(IndexRecord* record) {
    if (record == NULL) {
        printf("Record not found\n");
        return;
    }
    
    printf("Record ID: %d, Name: %s, Age: %d\n", record->id, record->name, record->age);
}

// Function to test the B-tree with IndexRecords
void test_btree_index() {
    // Create a B-tree
    BTree* tree = btree_create();
    
    // Insert some test records
    printf("Inserting 15 test records...\n");
    const char* names[] = {
        "Alice", "Bob", "Charlie", "David", "Eve",
        "Frank", "Grace", "Heidi", "Ivan", "Judy",
        "Kevin", "Laura", "Michael", "Nancy", "Oliver"
    };
    
    for (int i = 0; i < 15; i++) {
        int id = i * 10 + 100;
        IndexRecord* record = create_record(id, names[i], 20 + i);
        btree_insert(tree, id, record, sizeof(IndexRecord));
        free(record); // The tree makes a copy
    }
    
    // Print the tree structure
    btree_print(tree);
    
    // Test searching for records
    printf("\nSearching for records...\n");
    
    // Test existing keys
    Value* value = btree_search(tree, 100);
    if (value != NULL) {
        IndexRecord* record = (IndexRecord*)value->data;
        print_record(record);
    }
    
    value = btree_search(tree, 150);
    if (value != NULL) {
        IndexRecord* record = (IndexRecord*)value->data;
        print_record(record);
    }
    
    // Test non-existing key
    value = btree_search(tree, 999);
    if (value == NULL) {
        printf("Record with ID 999 not found (as expected)\n");
    }
    
    // Test deleting records
    printf("\nDeleting records with IDs 130 and 150...\n");
    btree_delete(tree, 130);
    btree_delete(tree, 150);
    
    // Print the tree after deletion
    btree_print(tree);
    
    // Try searching for a deleted key
    value = btree_search(tree, 150);
    if (value == NULL) {
        printf("Record with ID 150 not found after deletion (as expected)\n");
    }
    
    // Insert more records to test splitting and merging
    printf("\nInserting more records to test tree balancing...\n");
    for (int i = 15; i < 30; i++) {
        int id = i * 10 + 100;
        char name[50];
        sprintf(name, "User%d", i);
        IndexRecord* record = create_record(id, name, 20 + i);
        btree_insert(tree, id, record, sizeof(IndexRecord));
        free(record); // The tree makes a copy
    }
    
    // Print the tree again
    btree_print(tree);
    
    // Delete records in succession to force merging
    printf("\nDeleting multiple records to test node merging...\n");
    for (int i = 0; i < 10; i++) {
        btree_delete(tree, (i * 10) + 100);
    }
    
    // Print the tree after mass deletion
    btree_print(tree);
    
    // Clean up
    btree_destroy(tree);
    printf("\nB-tree index test completed.\n");
}

//---------------------------------------------------------------
// Main function for testing
//---------------------------------------------------------------

int main() {
    test_btree_index();
    return 0;
}