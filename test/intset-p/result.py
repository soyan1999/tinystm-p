import array


def get_inf(a):
    sum = num = 0
    min_ = float('inf')
    max_ = float('-inf')
    for i, num_ in enumerate(a):
        num += num_
        sum += i * num_
        if num_ != 0 and i < min_:
            min_ = i
        if num_ != 0 and i > max_:
            max_ = i
    return sum / num, min_, max_


v_log_max = 2000 + 1
group_size_max = 2000 + 1
group_commit_max = 1000 + 1
flush_max = 65 + 1
delay_max = 10000 + 1

v_log_collect = array.array('Q')
group_size_collect = array.array('Q')
group_commit_collect = array.array('Q')
flush_size_collect = array.array('Q')
delay_time_collect = array.array('Q')

with open('./result.bin', 'rb') as f:
    v_log_collect.fromfile(f, v_log_max)
    group_size_collect.fromfile(f, group_size_max)
    group_commit_collect.fromfile(f, group_commit_max)
    flush_size_collect.fromfile(f, flush_max)
    delay_time_collect.fromfile(f, delay_max)

print(get_inf(v_log_collect))
print(get_inf(group_size_collect))
print(get_inf(group_commit_collect))
print(get_inf(flush_size_collect))
print(get_inf(delay_time_collect))
