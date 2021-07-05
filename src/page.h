# ifndef _PAGE_H_
# define _PAGE_H_

# include "stm_internal.h"

# define PAGE_LENGTH    12
# define PAGE_SIZE      (1 << PAGE_LENGTH)                  // 4K
# define NVM_LENGTH     30
# define DRAM_LENGTH    30
# define VPN_NUM        (1 << (NVM_LENGTH - PAGE_LENGTH))   // 64k
# define PPN_LENGTH     (DRAM_LENGTH - PAGE_LENGTH)         // 18
# define VPN_LENGTH     (NVM_LENGTH - PAGE_LENGTH)          // 18
# define PPN_NUM        (1 << (DRAM_LENGTH - PAGE_LENGTH))  // 64k

typedef union v_page_inf { // 页表有效信息放在易失页表项可以利用CAS操作，替换与映射将操作同一变量
    struct {
        uint64_t vaild : 1;
        uint64_t used : 63; // thread used bitmap
    };
    uint64_t v_page_inf;
} v_page_inf_t;

typedef struct free_page_entry {
    struct free_page_entry *next;
    volatile v_page_inf_t page_inf;        
    uint64_t PPN;
    uint64_t VPN;
} free_page_entry_t;

struct free_page_head {
    uint32_t free_num;
    pthread_spinlock_t lock;
    free_page_entry_t *head;
} free_page_head;

typedef struct page_entry {
    uint64_t touch_id;
    free_page_entry_t *free_page;
} page_entry_t;


void page_init();
uint64_t *page_use(stm_tx_t *tx, uint64_t nv_addr);
void page_free(stm_tx_t *tx, uint64_t nv_addr, uint64_t commit_timestamp);

# include "stm_internal.h"
// global
page_entry_t page_table[VPN_NUM];

static inline uint64_t v_page_alloc() {
    return (uint64_t)aligned_alloc(PAGE_SIZE, PAGE_SIZE) >> PAGE_LENGTH;
}

static inline uint64_t *addr_nv_2_v(uint64_t PPN, uint64_t nv_addr) {
    return (uint64_t *)((PPN << PAGE_LENGTH) | ((PAGE_SIZE - 1) & nv_addr));
}

// move a node from tail to head
static inline void free_page_roll() {
    free_page_head.head = free_page_head.head->next;
}

static int page_map_(stm_tx_t *tx, uint64_t nv_addr) {
    pthread_spin_lock(&free_page_head.lock);

    // check if other thread has mapped the nv_page
    uint64_t VPN = nv_addr >> PAGE_LENGTH;
    if (page_table[VPN].free_page != NULL && page_table[VPN].free_page->VPN == VPN && page_table[VPN].free_page->page_inf.vaild) {
        pthread_spin_unlock(&free_page_head.lock);
        return 0;
    }
    
    // check if touchid is bigger than reproduce timestamp
    if (page_table[VPN].touch_id > _tinystm.addition.root->reproduce_timestamp) {
        pthread_spin_unlock(&free_page_head.lock);
        return -1;
    }

    v_page_inf_t old_v, new_v;
    
    while (1) {
        old_v = free_page_head.head->page_inf;
        new_v.v_page_inf = 0;

        // v_page in used
        if (old_v.used != 0 || ATOMIC_CAS_FULL(&free_page_head.head->page_inf.v_page_inf, old_v.v_page_inf, new_v.v_page_inf) == 0) {
            free_page_roll();
            continue;
        }
        
        // not used but mapped, clean the old page_table entry
        if (old_v.vaild == 1) {
            page_table[free_page_head.head->VPN].free_page = NULL;
        }

        // map to new nv_page 
        free_page_head.head->VPN = VPN;
        new_v.vaild = 1;
        new_v.used = 1 << tx->addition.thread_nb;
        free_page_head.head->page_inf = new_v;

        free_page_roll();
        break;
    }
    
    pthread_spin_unlock(&free_page_head.lock);
    return 0;
}

static void page_map(stm_tx_t *tx, uint64_t nv_addr) {
    while (page_map_(tx,nv_addr) < 0) {
        nv_log_reproduce();
    }
}

void page_init() {
    free_page_entry_t *node;

    free_page_head.free_num = PPN_NUM;
    pthread_spin_init(&free_page_head.lock, 0);

    node = free_page_head.head;
    for (int i = 0; i < PPN_NUM; i++) {
        node = malloc(sizeof(free_page_entry_t));
        node->PPN = v_page_alloc();
        node->VPN = 0;
        node->page_inf.v_page_inf = 0;
        if (i != PPN_NUM - 1) node = node->next;
    }
    node->next = free_page_head.head;
}

// map the page and set write set; if page has mapped, return directly
uint64_t *page_use(stm_tx_t *tx, uint64_t nv_addr) {
    uint64_t VPN = nv_addr >> PAGE_LENGTH;
    free_page_entry_t *page_entry = page_table[VPN].free_page;
    v_page_inf_t old_v, new_v, fail_v;
    
    // nv_page not mapped to v_page
    if (page_entry == NULL || page_entry->page_inf.vaild == 0) {
        page_map(tx,nv_addr);
        return addr_nv_2_v(page_entry->PPN, nv_addr);
    }
        
    // in write set, return directly
    old_v = page_entry->page_inf;
    if ((old_v.used & (1 << tx->addition.thread_nb)) != 0) return addr_nv_2_v(page_entry->PPN, nv_addr);

    new_v = page_entry->page_inf;
    new_v.used |= 1 << tx->addition.thread_nb;

    // CAS fail
    while (ATOMIC_CAS_FULL(&page_entry->page_inf.v_page_inf, old_v.v_page_inf, new_v.v_page_inf) == 0){
        fail_v = page_entry->page_inf;
        
        // page unmapped
        if (fail_v.vaild == 0) {
            page_map(tx,nv_addr);
            return addr_nv_2_v(page_entry->PPN, nv_addr);
        }

        if (page_table[VPN].free_page == NULL) {
            page_map(tx,nv_addr);
            return addr_nv_2_v(page_entry->PPN, nv_addr);
        }

        // other tx has changed the write set
        old_v = page_entry->page_inf;
        new_v = page_entry->page_inf;
        new_v.used |= 1 << tx->addition.thread_nb;
    }

    return addr_nv_2_v(page_entry->PPN, nv_addr);
}

// remove tx from write set when tx committed or abort and update touvh id
void page_free(stm_tx_t *tx, uint64_t nv_addr, uint64_t commit_timestamp) {
    uint64_t VPN = nv_addr >> PAGE_LENGTH;
    free_page_entry_t *page_entry = page_table[VPN].free_page;
    v_page_inf_t old_v, new_v;
    uint64_t new_timestamp = commit_timestamp, old_timestamp;

    // CAS modify write set
    do {
        old_v = page_entry->page_inf;
        if ((old_v.used & (1 << tx->addition.thread_nb)) != 0) break;
        new_v = page_entry->page_inf;
        new_v.used &= ~(1 << tx->addition.thread_nb);
    } while (ATOMIC_CAS_FULL(&page_entry->page_inf.v_page_inf, old_v.v_page_inf, new_v.v_page_inf) == 0);

    do {
        old_timestamp = page_table[VPN].touch_id;
        if (new_timestamp <= old_timestamp || new_timestamp == 0) break;
    } while (ATOMIC_CAS_FULL(&page_table[VPN].touch_id, old_timestamp, new_timestamp) == 0);
}
# endif /* _PAGE_H_ */