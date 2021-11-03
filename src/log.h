# ifndef _LOG_H_
# define _LOG_H_

# include "stm_internal.h"
# define V_LOG_LENGTH 15
# define V_LOG_TABLE_LOG_LENGTH 20
# ifndef SMALL_POOL
# define NVM_LENGTH     30
# define DRAM_LENGTH    30
# else
# define NVM_LENGTH     27
# define DRAM_LENGTH    27
# endif
# define V_LOG_TABLE_LENGTH (1 << V_LOG_TABLE_LOG_LENGTH)
# define V_LOG_ADDR_SHIFT (NVM_LENGTH - V_LOG_TABLE_LOG_LENGTH - 3)
# define MAX_UNPERSIST_TRANSACTION 100

# define NV_LOG_LENGTH 63
# define TYPE_NV_LOG_BLOCK 1

# define NV_LOG_BLOCK_NUM 1024
# define BEGIN_SIG 0xffffffffffffffff
# define END_SIG 0xfffffffffffffffe

# define LAYOUT_NAME "dudetm"
# ifndef SMALL_POOL
# define POOL_SIZE (1 * 1024 * 1024 * 1024)
# else
# define POOL_SIZE (128 * 1024 * 1024)
# endif


typedef uint64_t nv_ptr;

struct root {
    nv_ptr obj_root[127];
    uint64_t root_num;

    nv_ptr persist_block;
    nv_ptr reproduce_block;
    uint64_t persist_offset;
    uint64_t reproduce_offset;
    uint64_t persist_timestamp;
    uint64_t reproduce_timestamp;
};

typedef struct nv_log_entry {
    nv_ptr nv_addr;
    uint64_t data;
} nv_log_entry_t;


struct nv_log_block {
    nv_ptr next;
    uint64_t reserved;
    nv_log_entry_t logs[NV_LOG_LENGTH];
};


TOID_DECLARE_ROOT(struct root);
TOID_DECLARE(struct nv_log_block, TYPE_NV_LOG_BLOCK);

PMEMobjpool *pmem_init(char *pool_path); // use to init pmem area

struct nv_log {
    nv_ptr write_block;
    nv_ptr read_block;
    uint64_t write_offset;
    uint64_t read_offset;
    uint64_t last_timestamp;

};

typedef struct v_log_entry {
    uint64_t nv_addr;
    uint64_t data;
} v_log_entry_t;

struct v_log_block {
    uint64_t num;                       // only vaild in first entry
    struct v_log_block *next;
    v_log_entry_t v_logs[V_LOG_LENGTH];
};

// typedef struct v_log_pool {
//     uint64_t num;
//     struct v_log_block *first;
//     struct v_log_block *last;
// } v_log_pool_t;

typedef struct v_log_table_entry {
    nv_ptr addr;
    uint64_t value;
    struct v_log_table_entry *next;
} v_log_table_entry_t;

typedef struct nv_log_begin {
    uint64_t begin_flag;
    uint64_t length;
} nv_log_begin_t;

typedef struct nv_log_end {
    uint64_t end_flag;
    uint64_t time_commit;
} nv_log_end_t;


// void v_log_pool_init(); // alloc v_log_blocks to pool

void v_log_insert(stm_tx_t *tx, uint64_t nv_addr, uint64_t data); // use when write data to a new addr

void v_log_insert_exist(stm_tx_t *tx, uint64_t nv_addr, uint64_t data, uint64_t nb); //use when write data to a exist addr

void v_log_init(stm_tx_t *tx); // use when init tx thread

void v_log_reset(stm_tx_t *tx); // use when exit tx 

void v_log_table_init(); // init hashtable

void v_log_table_insert(v_log_entry_t *entry); // insert v_log

void v_log_table_move(); // move v_log to hash_table

void v_log_table_clean(); // insert v_log

int v_log_table_persist(); // persist v_log in hash_table

int log_before_commit(stm_tx_t *tx, uint64_t commit_timestamp); // add hashtable or persist log

static int nv_log_insert(uint64_t *entry, int state);

void nv_log_init(); // use when init stm

int nv_log_record(stm_tx_t *tx, uint64_t commit_timestamp); // use when commit

int nv_log_reproduce(); // use after commit

void nv_log_save(); // save all log to nv_heap

void nv_log_flush(); // flush v_log


static void v_log_expand(stm_tx_t *tx) {
    v_log_block_t *node = tx->addition.v_log_block, *prev = NULL;

    while (node != NULL) {
        prev = node;
        node = node->next;
    }

    node = (v_log_block_t *)malloc(sizeof(v_log_block_t));
    node->num = 0;
    node->next = NULL;
    if (prev != NULL) prev->next = node;
    else tx->addition.v_log_block = node;
}

void v_log_table_init() {
    v_log_table_entry_t **v_log_table = (v_log_table_entry_t **)calloc(V_LOG_TABLE_LENGTH, sizeof(v_log_table_entry_t *));
    uint64_t *v_log_modified = (uint64_t *)calloc(V_LOG_TABLE_LENGTH, sizeof(uint64_t));
    _tinystm.addition.v_log_table = v_log_table;
    _tinystm.addition.v_log_modified = v_log_modified;
    _tinystm.addition.v_log_modified_count = 0;
    _tinystm.addition.v_log_count = 0;
    _tinystm.addition.v_log_transaction_count = 0;
    _tinystm.addition.max_timestamp = 0;
}

void v_log_table_insert(v_log_entry_t *entry) {
    uint64_t nv_addr = entry->nv_addr, data = entry->data;
    uint64_t index = (nv_addr >> 3) & (~(~0 << V_LOG_ADDR_SHIFT)); //low 10~3 bit as index
    v_log_table_entry_t **v_log_table = _tinystm.addition.v_log_table;
    uint64_t *v_log_modified = _tinystm.addition.v_log_modified;
    v_log_table_entry_t *v_log_table_entry;

    if (v_log_table[index] == NULL || v_log_table[index]->addr > nv_addr) {
        v_log_table_entry = (v_log_table_entry_t *)malloc(sizeof(v_log_table_entry_t));
        v_log_table_entry->addr = nv_addr;
        v_log_table_entry->value = data;
        v_log_table_entry->next = v_log_table[index];

        if (v_log_table[index] == NULL) v_log_modified[_tinystm.addition.v_log_modified_count ++] = index;
        _tinystm.addition.v_log_count ++;
        v_log_table[index] = v_log_table_entry;
    }
    else {
        v_log_table_entry_t *prev, *next;
        prev = v_log_table[index];
        next = prev->next;
        while (1) {
            if (prev->addr == nv_addr) {
                prev->value = data;
                break;
            }
            else if (next == NULL || next->addr > nv_addr) {
                v_log_table_entry = (v_log_table_entry_t *)malloc(sizeof(v_log_table_entry_t));
                v_log_table_entry->addr = nv_addr;
                v_log_table_entry->value = data;
                v_log_table_entry->next = next;
                prev->next = v_log_table_entry;

                _tinystm.addition.v_log_count ++;
                break;
            }
            else {
                prev = next;
                next = next->next;
            }
        }
    }
}

void v_log_table_move(stm_tx_t *tx) {
    v_log_block_t  *v_log = tx->addition.v_log_block;
    
    for (int record_num = 0; record_num < tx->addition.v_log_block->num; record_num++) {
        if (record_num != 0 && record_num % V_LOG_LENGTH == 0) v_log = v_log->next;
        v_log_table_insert(&(v_log->v_logs[record_num % V_LOG_LENGTH]));
    }
}

void v_log_table_clean() {
    v_log_table_entry_t *prev, *next;
    uint64_t index;

    for (uint64_t i = 0; i < _tinystm.addition.v_log_modified_count; i ++) {
        index = _tinystm.addition.v_log_modified[i];
        prev = _tinystm.addition.v_log_table[index];
        next = prev->next;
        while (1) {
            free(prev);
            if (next == NULL) break;
            prev = next;
            next = next->next;
        }
        _tinystm.addition.v_log_table[index] = NULL;
    }

    _tinystm.addition.v_log_count = 0;
    _tinystm.addition.v_log_modified_count = 0;
    _tinystm.addition.v_log_transaction_count = 0;
}

int v_log_table_persist() {
    uint64_t commit_timestamp = _tinystm.addition.max_timestamp;
    nv_log_begin_t begin_block = {.begin_flag = BEGIN_SIG, .length = _tinystm.addition.v_log_count};
    nv_log_end_t end_block = {.end_flag = END_SIG, .time_commit = commit_timestamp + _tinystm.addition.nv_log->last_timestamp};
    v_log_entry_t v_log_entry;
    // backup of write ptr
    uint64_t write_offset = _tinystm.addition.nv_log->write_offset;
    uint64_t write_block = _tinystm.addition.nv_log->write_block;

    v_log_table_entry_t *prev, *next;
    uint64_t index;
    int result = 0;
    
    // insert begin block
    result = nv_log_insert((uint64_t *)&begin_block, 0);
    if (result != 0) {
        _tinystm.addition.nv_log->write_offset = write_offset;
        _tinystm.addition.nv_log->write_block = write_block;
        return result;
    }
    
    // insert main logs
    for (uint64_t i = 0; i < _tinystm.addition.v_log_modified_count; i ++) {
        index = _tinystm.addition.v_log_modified[i];
        prev = _tinystm.addition.v_log_table[index];
        next = prev->next;
        while (1) {
            v_log_entry.nv_addr = prev->addr, 
            v_log_entry.data = prev->value;
            result = nv_log_insert((uint64_t *)(&v_log_entry), 1);
            if (result != 0) {
                _tinystm.addition.nv_log->write_offset = write_offset;
                _tinystm.addition.nv_log->write_block = write_block;
                return result;
            }

            if (next == NULL) break;
            prev = next;
            next = next->next;
        }
    }

    // insert end block
    result = nv_log_insert((uint64_t *)&end_block, 2);
    if (result != 0) {
        _tinystm.addition.nv_log->write_offset = write_offset;
        _tinystm.addition.nv_log->write_block = write_block;
        return result;
    }

    //if (_tinystm.addition.nv_log->write_offset != 0) 
    //    pmemobj_flush(_tinystm.addition.pool, (void *)_tinystm.addition.nv_log->write_block, sizeof(struct nv_log_block)); // flush
    
    pmemobj_drain(_tinystm.addition.pool);
    
    // persist log inf in root
    struct pobj_action act[3];
    pmemobj_set_value(_tinystm.addition.pool, &act[0], &_tinystm.addition.root->persist_block, _tinystm.addition.nv_log->write_block);
    pmemobj_set_value(_tinystm.addition.pool, &act[1], &_tinystm.addition.root->persist_offset, _tinystm.addition.nv_log->write_offset);
    pmemobj_set_value(_tinystm.addition.pool, &act[2], &_tinystm.addition.root->persist_timestamp, commit_timestamp + _tinystm.addition.nv_log->last_timestamp);
    pmemobj_publish(_tinystm.addition.pool, act, 3);

    //tx->addition.v_log_block->num = 0; // delete v_log
    return 0;
}

int log_before_commit(stm_tx_t *tx, uint64_t commit_timestamp) {
    v_log_table_move(tx);
    _tinystm.addition.max_timestamp = commit_timestamp;
    _tinystm.addition.v_log_transaction_count ++;
    if (_tinystm.addition.v_log_transaction_count >= MAX_UNPERSIST_TRANSACTION || _tinystm.addition.v_log_count >= NV_LOG_LENGTH) {
        collect_before_log_start(tx);
        while(v_log_table_persist()) {
            nv_log_reproduce();
        }
        collect_before_commit(tx, 1, _tinystm.addition.v_log_count);
        v_log_table_clean();
        if (_tinystm.addition.v_log_count != 0) nv_log_reproduce();
        return 1;
    }
    return 0;
}

// void v_log_pool_init() {
//     v_log_pool_t *v_log_pool = (v_log_pool_t *)malloc(sizeof(v_log_pool_t));
//     v_log_pool->first = (v_log_block_t *)malloc(sizeof(v_log_block_t));
//     v_log_block_t *node = v_log_pool->first;

//     for (uint64_t i = 1; i < V_LOG_NUM; i ++) {
//         node->num = 0;
//         node->next = (v_log_block_t *)malloc(sizeof(v_log_block_t));
//         node = node->next;
//     }
//     node->num = 0;
//     node->next = NULL;
//     v_log_pool->last = node;

//     _tinystm.addition.v_log_pool = v_log_pool;
// }

void v_log_init(stm_tx_t *tx) {
    v_log_expand(tx);
}

void v_log_insert_exist(stm_tx_t *tx, uint64_t nv_addr, uint64_t data, uint64_t nb) {
    v_log_block_t *node = tx->addition.v_log_block;

    while (nb > V_LOG_LENGTH - 1) {
        nb -= V_LOG_LENGTH;
        node = node->next;
    }

    node->v_logs[nb].nv_addr = nv_addr;
    node->v_logs[nb].data = data;
}

void v_log_insert(stm_tx_t *tx, uint64_t nv_addr, uint64_t data) {
    v_log_block_t *node = tx->addition.v_log_block;
    unsigned int nb = node->num;

    while (nb > V_LOG_LENGTH - 1) {
        nb -= V_LOG_LENGTH;
        if (node ->next == NULL) v_log_expand(tx);
        node = node->next;
    }

    node->v_logs[nb].nv_addr = nv_addr;
    node->v_logs[nb].data = data;
    tx->addition.v_log_block->num ++;
}

void v_log_reset(stm_tx_t *tx) {
    tx->addition.v_log_block->num = 0;
}

// persist log operation

static void nv_log_alloc() {
    PMEMoid Temp, Next;
    struct nv_log_block *temp, *next;
    TX_BEGIN(_tinystm.addition.pool) {
        pmemobj_tx_add_range_direct(&_tinystm.addition.root->persist_block, 2 * sizeof(nv_ptr));
        Temp = pmemobj_tx_zalloc(sizeof(struct nv_log_block), TYPE_NV_LOG_BLOCK);
        temp = pmemobj_direct(Temp);
        _tinystm.addition.root->persist_block = Temp.off;
        _tinystm.addition.root->reproduce_block = Temp.off;

        for (int i = 0; i < NV_LOG_BLOCK_NUM - 1; i++) {
            Next = pmemobj_tx_zalloc(sizeof(struct nv_log_block), TYPE_NV_LOG_BLOCK);
            next = pmemobj_direct(Next);

            temp->next = Next.off;
            Temp = Next;
            temp = next;
        }
        temp->next = _tinystm.addition.root->persist_block;

    }TX_END
}

static void nv_log_get(v_log_entry_t *entry) {
    struct nv_log_block *temp;
    temp = (struct nv_log_block *)(_tinystm.addition.nv_log->read_block + _tinystm.addition.base);

    entry->nv_addr = temp->logs[_tinystm.addition.nv_log->read_offset].nv_addr;
    entry->data = temp->logs[_tinystm.addition.nv_log->read_offset++].data;

    if (_tinystm.addition.nv_log->read_offset == NV_LOG_LENGTH) {
        _tinystm.addition.nv_log->read_block = temp->next;
        _tinystm.addition.nv_log->read_offset = 0;
    }
}

static int nv_log_insert(uint64_t *entry, int state) { //state mean 
    struct nv_log_block *temp;
    static uint64_t begin_off;
    if (state == 0) 
        begin_off = _tinystm.addition.nv_log->write_offset;

    temp = (struct nv_log_block *)(_tinystm.addition.nv_log->write_block + _tinystm.addition.base);

    temp->logs[_tinystm.addition.nv_log->write_offset].nv_addr = *entry;
    temp->logs[_tinystm.addition.nv_log->write_offset].data = *(entry + 1);
    // pmemobj_flush(_tinystm.addition.pool, &temp->logs[_tinystm.addition.nv_log->write_offset], 2 * sizeof(uint64_t)); // flush
    _tinystm.addition.nv_log->write_offset ++;

    if (_tinystm.addition.nv_log->write_offset == NV_LOG_LENGTH) {
        if (temp->next == _tinystm.addition.nv_log->read_block) return -1;
        collect_before_log_flush(NV_LOG_LENGTH - begin_off);
        pmemobj_flush(_tinystm.addition.pool, &temp->logs[begin_off], 2 * (NV_LOG_LENGTH - begin_off) * sizeof(uint64_t)); // flush
        //if (temp->next == _tinystm.addition.nv_log->read_block) return -1;
        // pmemobj_flush(_tinystm.addition.pool, (void *)(_tinystm.addition.nv_log->write_block), 2 *sizeof(uint64_t)); // flush
        _tinystm.addition.nv_log->write_block = temp->next;
        _tinystm.addition.nv_log->write_offset = 0;
        begin_off = 0;
        return 0;
    }
    else if (state == 2) {
        collect_before_log_flush(_tinystm.addition.nv_log->write_offset - begin_off);
        pmemobj_flush(_tinystm.addition.pool, &temp->logs[begin_off], 2 * (_tinystm.addition.nv_log->write_offset - begin_off) * sizeof(uint64_t));
    }
    return 0;
}

static void nv_log_recovery() {
    while (_tinystm.addition.root->persist_timestamp > _tinystm.addition.root->reproduce_timestamp) {
        nv_log_reproduce();
    }
}

void nv_log_init() {
    if (_tinystm.addition.root->persist_block == 0) {
        nv_log_alloc();
        _tinystm.addition.nv_log->read_block = _tinystm.addition.root->persist_block;
        _tinystm.addition.nv_log->write_block = _tinystm.addition.root->reproduce_block;
    }
    else {
        _tinystm.addition.nv_log->read_block = _tinystm.addition.root->reproduce_block;
        _tinystm.addition.nv_log->write_block = _tinystm.addition.root->persist_block;
        _tinystm.addition.nv_log->read_offset = _tinystm.addition.root->reproduce_offset;
        _tinystm.addition.nv_log->write_offset = _tinystm.addition.root->persist_offset;
        _tinystm.addition.nv_log->last_timestamp = _tinystm.addition.root->persist_timestamp;
        nv_log_recovery();
    }
}

int nv_log_record(stm_tx_t *tx, uint64_t commit_timestamp) {
    if(tx->addition.v_log_block->num == 0) return 0;
    nv_log_begin_t begin_block = {.begin_flag = BEGIN_SIG, .length = tx->addition.v_log_block->num};
    nv_log_end_t end_block = {.end_flag = END_SIG, .time_commit = commit_timestamp + _tinystm.addition.nv_log->last_timestamp};
    // backup of write ptr
    uint64_t write_offset = _tinystm.addition.nv_log->write_offset;
    uint64_t write_block = _tinystm.addition.nv_log->write_block;
    v_log_block_t  *v_log = tx->addition.v_log_block;
    int result = 0;
    
    // insert begin block
    result = nv_log_insert((uint64_t *)&begin_block, 0);
    if (result != 0) {
        _tinystm.addition.nv_log->write_offset = write_offset;
        _tinystm.addition.nv_log->write_block = write_block;
        return result;
    }
    
    // insert main logs
    for (int record_num = 0; record_num < tx->addition.v_log_block->num; record_num++) {
        if (record_num != 0 && record_num % V_LOG_LENGTH == 0) v_log = v_log->next;
        result = nv_log_insert((uint64_t *)(&(v_log->v_logs[record_num % V_LOG_LENGTH])), 1);
        if (result != 0) {
            _tinystm.addition.nv_log->write_offset = write_offset;
            _tinystm.addition.nv_log->write_block = write_block;
            return result;
        }
        // if (record_num != 0 && record_num % V_LOG_LENGTH == 0) v_log = v_log->next;
    }

    // insert end block
    result = nv_log_insert((uint64_t *)&end_block, 2);
    if (result != 0) {
        _tinystm.addition.nv_log->write_offset = write_offset;
        _tinystm.addition.nv_log->write_block = write_block;
        return result;
    }

    //if (_tinystm.addition.nv_log->write_offset != 0) 
    //    pmemobj_flush(_tinystm.addition.pool, (void *)_tinystm.addition.nv_log->write_block, sizeof(struct nv_log_block)); // flush
    
    pmemobj_drain(_tinystm.addition.pool);
    
    // persist log inf in root
    struct pobj_action act[3];
    pmemobj_set_value(_tinystm.addition.pool, &act[0], &_tinystm.addition.root->persist_block, _tinystm.addition.nv_log->write_block);
    pmemobj_set_value(_tinystm.addition.pool, &act[1], &_tinystm.addition.root->persist_offset, _tinystm.addition.nv_log->write_offset);
    pmemobj_set_value(_tinystm.addition.pool, &act[2], &_tinystm.addition.root->persist_timestamp, commit_timestamp + _tinystm.addition.nv_log->last_timestamp);
    pmemobj_publish(_tinystm.addition.pool, act, 3);

    //tx->addition.v_log_block->num = 0; // delete v_log
    return 0;
}

int nv_log_reproduce() {
    if (_tinystm.addition.root->persist_timestamp == _tinystm.addition.root->reproduce_timestamp) return 0;

    // uint64_t read_offset = _tinystm.addition.nv_log->read_offset;
    // uint64_t read_block = _tinystm.addition.nv_log->read_block;
    v_log_entry_t temp;
    uint64_t log_length, commit_timestamp;

    // read begin block and get log length
    nv_log_get(&temp);
    //if (temp.nv_addr != BEGIN_SIG) return -1; // not the begin block
    assert(temp.nv_addr == BEGIN_SIG);
    log_length = temp.data;

    // read log and persist real data
    for (int i = 0; i < log_length; i ++) {
        nv_log_get(&temp);
        *((uint64_t *)(temp.nv_addr + _tinystm.addition.base)) = temp.data;
        pmemobj_flush(_tinystm.addition.pool, (void *)(temp.nv_addr + _tinystm.addition.base), sizeof(uint64_t));
    }
    
    pmemobj_drain(_tinystm.addition.pool);
    // read end block and persist metadata in root
    nv_log_get(&temp);
    //if (temp.nv_addr != END_SIG) return -1; // not the begin block
    assert(temp.nv_addr == END_SIG);
    commit_timestamp = temp.data;

    struct pobj_action act[3];
    pmemobj_set_value(_tinystm.addition.pool, &act[0], &_tinystm.addition.root->reproduce_block, _tinystm.addition.nv_log->read_block);
    pmemobj_set_value(_tinystm.addition.pool, &act[1], &_tinystm.addition.root->reproduce_offset, _tinystm.addition.nv_log->read_offset);
    pmemobj_set_value(_tinystm.addition.pool, &act[2], &_tinystm.addition.root->reproduce_timestamp, commit_timestamp);
    pmemobj_publish(_tinystm.addition.pool, act, 3);

    return 0;
}

void nv_log_save() {
    nv_log_flush();
    pmemobj_close(_tinystm.addition.pool);
}

void nv_log_flush() {
    while(v_log_table_persist()) {
        nv_log_reproduce();
    }
    v_log_table_clean();
    nv_log_recovery();
}

PMEMobjpool *pmem_init(char *pool_path) {
    FILE *r = fopen(pool_path, "r");
    PMEMoid Root;

    if (r == NULL) {
        PMEMobjpool *pop = pmemobj_create(pool_path, LAYOUT_NAME, POOL_SIZE, 0666);
        _tinystm.addition.pool = pop;
        Root = pmemobj_root(pop, sizeof(struct root));
        _tinystm.addition.root = pmemobj_direct(Root);
        _tinystm.addition.base = (uint64_t)_tinystm.addition.root - Root.off;
        _tinystm.addition.nv_log = calloc(1, sizeof(nv_log_t));
        v_log_table_init();
        nv_log_init();
    }
    else {
        fclose(r);
        PMEMobjpool *pop = pmemobj_open(pool_path, LAYOUT_NAME);
        _tinystm.addition.pool = pop;
        Root = pmemobj_root(pop, sizeof(struct root));
        _tinystm.addition.root = pmemobj_direct(Root);
        _tinystm.addition.base = (uint64_t)_tinystm.addition.root - Root.off;
        _tinystm.addition.nv_log = calloc(1, sizeof(nv_log_t));
        v_log_table_init();
        nv_log_init();
    }
    init_measure();
    return _tinystm.addition.pool;
}
# endif /* _LOG_H_ */