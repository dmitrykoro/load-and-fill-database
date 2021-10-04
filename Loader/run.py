import util
import load_db

NUM_OF_THREADS = 3
NUM_OF_QUERIES = 100
NUM_OF_ITERATIONS = 5
TIMEDELTA = 1.0  # sec

single_thread_without_indexes_stats = [[], []]
single_thread_with_indexes_stats = [[], []]

if __name__ == '__main__':
    # drop indexes, run w/o indexes, create indexes, run w/indexes

    cursor, conn = util.connect()
    util.drop_indexes(cursor)
    cursor.execute('set enable_hashjoin to on;')

    '''
    description_before = (
        f'Without indexes. Single thread, number of queries {NUM_OF_QUERIES} to {NUM_OF_QUERIES * NUM_OF_ITERATIONS}'
    )
    load_db.load_single_thread(cursor, NUM_OF_QUERIES, TIMEDELTA, description_before)

    util.create_indexes(cursor)
    description_after = (
        f'With indexes. Single thread, number of queries {NUM_OF_QUERIES} to {NUM_OF_QUERIES * NUM_OF_ITERATIONS}'
    )
    load_db.load_single_thread(cursor, NUM_OF_QUERIES, TIMEDELTA, description_after)

    description = (
        f'With to without indexes. Single thread, number of queries {NUM_OF_QUERIES} to {NUM_OF_QUERIES * NUM_OF_ITERATIONS}'
    )
    util.print_relation_plot(description, description_before, description_after)
    '''

    ########## threads

    description_before = (
        f'Without indexes. Multi threads, number of threads {NUM_OF_THREADS} to {NUM_OF_THREADS * NUM_OF_ITERATIONS}'
    )
    load_db.load_multithread(NUM_OF_THREADS, description_before)

    util.create_indexes(cursor)
    cursor.execute('set enable_hashjoin to off;')

    description_after = (
        f'With indexes. Multi threads, number of threads {NUM_OF_THREADS} to {NUM_OF_THREADS * NUM_OF_ITERATIONS}'
    )
    load_db.load_multithread(NUM_OF_THREADS, description_after)
    description = (
        f'With to without indexes. Multi threads, number of threads {NUM_OF_THREADS} to {NUM_OF_THREADS * NUM_OF_ITERATIONS}'
    )
    util.print_relation_plot(description, description_before, description_after)
