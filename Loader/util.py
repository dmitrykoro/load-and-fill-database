import psycopg2
import matplotlib.pyplot as plt
import pickle

f = open('db_params/params.txt', 'r')
data = f.readlines()
file_lines = data[0].split(" ")

def connect():
    conn = psycopg2.connect(dbname=file_lines[0], user=file_lines[1], password=file_lines[2], host=file_lines[3])
    # print(f'Connected to database: {lines[0]}, host: {lines[3]}')
    return conn.cursor(), conn


def drop_indexes(cursor):
    cursor.execute("DROP INDEX IF EXISTS maintenance_vehicle")
    cursor.execute("DROP INDEX IF EXISTS managers")
    cursor.execute("DROP INDEX IF EXISTS service_number")
    cursor.execute("DROP INDEX IF EXISTS plant_id")
    print('Dropped indexes successfully')


def create_indexes(cursor):
    cursor.execute("CREATE INDEX maintenance_vehicle ON vehicle(id, specs_id)")
    cursor.execute("CREATE INDEX managers ON selling(seller_id)")
    cursor.execute("CREATE INDEX service_number ON maintenance(service_number)")
    cursor.execute("CREATE INDEX plant_id ON plant(id)")
    print('Created indexes successfully')


def print_plot(data, title, show=True):
    plt.plot(data[0], data[1])
    plt.title(title)
    if show:
        plt.show()


def print_relation_plot(title, filename_before, filename_after):
    data_before = pickle.load(open(f'dumps/{filename_before}.dump', 'rb'))
    plt.plot(data_before[0], data_before[1], label='Before')

    data_after = pickle.load(open(f'dumps/{filename_after}.dump', 'rb'))

    plt.plot(data_after[0], data_after[1], label='After')
    plt.title(title)
    plt.xlabel('Number of threads')
    plt.ylabel('Response time')
    plt.legend()
    plt.show()
