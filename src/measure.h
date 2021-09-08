# ifndef _MEASURE_H_
# define _MEASURE_H_

# include "stm_internal.h"



void init_measure();

void tx_init_measure(stm_tx_t *tx);

void collect_after_tx_start(stm_tx_t *tx); // collect start time

void collect_before_log_combine(stm_tx_t *tx); // collect v_log size

void collect_before_log_flush(uint64_t flush_size); // collect flush size

void collect_before_commit(stm_tx_t *tx, int if_flush, uint64_t commit_size); //collect delay and group size and combined size

void result_output(); //write result to file

void init_measure() {
    #ifdef ENABLE_MEASURE
    memset(&_tinystm.addition.global_measure, 0, sizeof(global_measure_t));
    #endif
}

void tx_init_measure(stm_tx_t *tx) {
    #ifdef ENABLE_MEASURE
    memset(&tx->addition.tx_measure, 0, sizeof(tx_measure_t));
    #endif
}

void collect_after_tx_start(stm_tx_t *tx) {
    #ifdef ENABLE_MEASURE
    if (!tx->attr.read_only) {
        gettimeofday(&tx->addition.tx_measure.start_time[tx->addition.tx_measure.group_size ++], NULL);
    }
    #endif
}

void collect_before_log_combine(stm_tx_t *tx) {
    #ifdef ENABLE_MEASURE
    uint64_t v_log_num = tx->addition.v_log_block->num;
    if (v_log_num < V_LOG_COLLECT_MAX) _tinystm.addition.global_measure.v_log_size_collect[v_log_num] ++;
    else _tinystm.addition.global_measure.v_log_size_collect[V_LOG_COLLECT_MAX] ++;
    #endif
}

void collect_before_log_flush(uint64_t flush_size) {
    #ifdef ENABLE_MEASURE
    if (flush_size < FLUSH_COLLECT_MAX) _tinystm.addition.global_measure.flush_size_collect[flush_size] ++;
    else _tinystm.addition.global_measure.flush_size_collect[FLUSH_COLLECT_MAX] ++;
    #endif
}

void collect_before_commit(stm_tx_t *tx, int if_flush, uint64_t commit_size) {
    #ifdef ENABLE_MEASURE
    struct timeval now_val;
    uint64_t delay, group_size;

    if (if_flush) {
        gettimeofday(&now_val, NULL);
        
        for(uint64_t i = 0; i < tx->addition.tx_measure.group_size; i ++) {
            delay = 1000000 * (now_val.tv_sec - tx->addition.tx_measure.start_time[i].tv_sec) + \
             now_val.tv_usec - tx->addition.tx_measure.start_time[i].tv_usec;
            
            if (delay < DELAY_COLLECT_MAX) _tinystm.addition.global_measure.delay_time_collect[delay] ++;
            else _tinystm.addition.global_measure.delay_time_collect[DELAY_COLLECT_MAX] ++;
        }

        if(commit_size < GROUP_COLLECT_MAX) _tinystm.addition.global_measure.group_size_collect[commit_size] ++;
        else _tinystm.addition.global_measure.group_size_collect[GROUP_COLLECT_MAX] ++;

        group_size = tx->addition.tx_measure.group_size;
        if(group_size < GROUP_COMMIT_MAX) _tinystm.addition.global_measure.group_commit_collect[group_size] ++;
        else _tinystm.addition.global_measure.group_commit_collect[GROUP_COMMIT_MAX] ++;

        tx->addition.tx_measure.group_size = 0;
    }
    #endif
}

void result_output() {
    #ifdef ENABLE_MEASURE
    FILE *f = fopen("./result.bin", "wb");
    fwrite(&_tinystm.addition.global_measure, sizeof(global_measure_t), 1, f);
    fclose(f);
    #endif
}

# endif /* _MEASURE_H_ */