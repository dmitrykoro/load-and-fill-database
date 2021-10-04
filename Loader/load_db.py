import concurrent.futures
import sched, time
import random
import pickle
import threading

import util
from run import NUM_OF_QUERIES, NUM_OF_ITERATIONS, NUM_OF_THREADS
from queries import query_list

s = sched.scheduler(time.time, time.sleep)
TOTAL_NUM_OF_QUERIES = NUM_OF_QUERIES * NUM_OF_ITERATIONS


def load(cursor, num_of_queries):
    print(f'Loading with {num_of_queries} queries')

    total_time = 0
    for i in range(num_of_queries):
        query = random.choice(query_list)
        cursor.execute(f'EXPLAIN ANALYZE {query}')
        execution_result = cursor.fetchall()
        execution_time = float(execution_result[-1][0].split(" ")[-2])
        planning_time = float(execution_result[-2][0].split(" ")[-2])
        total_time_per_query = execution_time + planning_time
        total_time += total_time_per_query

    avg_time = total_time / num_of_queries
    # print(f'Average time for {num_of_queries} queries is {avg_time}')
    return avg_time


def load_threads(cursor):
    global iter_stats
    num_of_queries = 30
    for i in range(num_of_queries):
        query = random.choice(query_list)
        cursor.execute(f'EXPLAIN ANALYZE {query}')
        execution_result = cursor.fetchall()
        execution_time = float(execution_result[-1][0].split(" ")[-2])
        planning_time = float(execution_result[-2][0].split(" ")[-2])

        with threading.Lock():
            iter_stats += execution_time + planning_time

    return


def load_single_thread(cursor, num_of_queries, timedelta, description_for_plot):
    global single_thread_stats
    if num_of_queries == NUM_OF_QUERIES:
        single_thread_stats = [[], []]

    stats = load(cursor, num_of_queries)

    single_thread_stats[0].append(num_of_queries)
    single_thread_stats[1].append(stats)

    if num_of_queries == TOTAL_NUM_OF_QUERIES:
        print('Finished')
        pickle.dump(single_thread_stats, open(f'dumps/{description_for_plot}.dump', 'wb'))
        return

    s.enter(delay=timedelta, priority=1, action=load_single_thread,
            argument=(
                cursor, int(num_of_queries + TOTAL_NUM_OF_QUERIES / NUM_OF_ITERATIONS), timedelta,
                description_for_plot))
    s.run()


def load_multithread(num_of_threads, description_for_plot):
    global iter_stats
    multithread_stats = [[], []]

    for i in range(NUM_OF_ITERATIONS):
        print(f'Loading on iteration {i} with {num_of_threads} threads')
        iter_stats = 0.0
        for j in range(num_of_threads):
            cursor, conn = util.connect()

            print(f'Connected for {j} thread in iteration {i}')
            thread = threading.Thread(target=load_threads, args=(cursor,))
            thread.start()

        time.sleep(3)

        print(f'Current iteration stats: {iter_stats}')
        multithread_stats[0].append(num_of_threads)
        multithread_stats[1].append(iter_stats / (num_of_threads))

        num_of_threads += NUM_OF_THREADS

    print('Finished')
    print(multithread_stats)
    pickle.dump(multithread_stats, open(f'dumps/{description_for_plot}.dump', 'wb'))
    return
