import psycopg2
import psycopg2.extras
import codecs
import random
import string
import linecache
import time
from datetime import datetime

cursor = None
conn = None


def init_variables():
    global number_of_clients
    number_of_clients = int(input("Input number of clients: "))

    global number_of_people
    number_of_people = int(input("Input number of workers: "))

    global number_of_vehicles
    number_of_vehicles = int(input("Input number of vehicles: "))


def get_surnames():
    global surnames
    surnames = []

    f = open('values/surnames.txt', 'r')

    i = 1
    while (True):
        cur_line = linecache.getline('values/surnames.txt', i)
        if (cur_line):
            surnames.append(cur_line)
        else:
            break
        i = i + 1
    f.close()


def get_colors():
    global colors
    colors = []

    f = open('values/colors.txt', 'r')
    colors = f.readlines()


def time_utility(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(format, time.localtime(ptime))


def get_random_date(start, end, prop):
    return time_utility(start, end, '%Y-%m-%d', prop)


def connect():
    f = open('db_params/params.txt', 'r')
    data = f.readlines()
    lines = data[0].split(" ")
    conn = psycopg2.connect(dbname=lines[0], user=lines[1], password=lines[2], host=lines[3])
    print('_connected')
    return conn


def initialise(conn):
    cursor = conn.cursor()
    print('_initialised')
    return cursor


def insert_clients(cursor, numberOfClients, conn):
    list_of_values = []

    for i in range(numberOfClients):
        current_surname = random.choice(surnames)
        current_phone = random.randint(10000000000, 99999999999)
        current_passport = random.randint(1000000000, 9999999999)

        value = (current_surname, current_phone, current_passport)
        list_of_values.append(value)

    query = 'INSERT INTO client (name, phone, passport_num) VALUES (%s, %s, %s)'
    psycopg2.extras.execute_batch(cursor, query, list_of_values)
    print('_inserted clients')
    conn.commit()


def insert_people(cursor, numberOfPeople, conn):
    for i in range(numberOfPeople):
        current_surname = random.choice(surnames)
        current_address = ''.join(
            random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for i in
            range(random.randint(1, 300)))
        current_phone = random.randint(10000000000, 99999999999)
        current_date = get_random_date("2000-01-01", "2020-05-17", random.random())

        cursor.execute('INSERT INTO people (people_name, address, phone, date_of_entry) VALUES (%s, %s, %s, %s);',
                       (current_surname, current_address, current_phone, current_date))

    print('_inserted people')
    conn.commit()


def insert_from_file(cursor, conn, filename, query):
    f = open(filename, 'r')

    for line in f:
        cursor.execute(query, (line,))

    print('_inserted ' + filename)
    conn.commit()
    f.close()


def insert_plant(cursor, conn):
    cursor.execute('SELECT id FROM plant_city')
    ids = cursor.fetchall()

    cursor.execute('SELECT COUNT(*) FROM plant')
    number_of_existing_plants = cursor.fetchone()

    number_of_plants = 50

    if number_of_existing_plants[0] < number_of_plants:
        for i in range(number_of_plants):
            current_plant_name = ''.join(
                random.choice(string.ascii_uppercase + string.ascii_lowercase) for i in range(random.randint(3, 30)))
            current_plant_city_id = random.choice(ids)

            cursor.execute('INSERT INTO plant (name, city) VALUES (%s, %s);',
                           (current_plant_name, current_plant_city_id))
        print('_inserted plants')
    print('_not inserted plants, already exists')
    conn.commit()


def insert_specs(cursor, conn):
    amount_of_specs = 1000

    cursor.execute('SELECT id FROM body')
    body_ids = cursor.fetchall()

    cursor.execute('SELECT COUNT(*) FROM specs')
    number_of_existing_specs = cursor.fetchone()

    cursor.execute('SELECT * FROM specs')
    inserted_specs = cursor.fetchall()

    if number_of_existing_specs[0] < amount_of_specs:
        for i in range(amount_of_specs):
            current_engine_code = random.choice(['EP6', 'C3L', 'H4M', 'K7M', 'F4R', 'K9K'])
            current_engine_volume = random.choice(['1.6', '1.4', '2.0', '2.5'])
            current_body = random.choice(body_ids)
            current_doors = random.randint(2, 5)
            current_transmission = random.choice(['Manual', 'AT', 'CVT'])
            current_drive = random.choice(['FWD', 'AWD'])

            current_specs = ''.join(str(current_engine_code)
                                    + str(current_engine_volume)
                                    + str(current_body)
                                    + str(current_doors)
                                    + str(current_transmission)
                                    + str(current_drive))

            if current_specs not in inserted_specs:
                cursor.execute('INSERT INTO specs (id, '
                               'engine_code, '
                               'engine_volume, '
                               'body_type, '
                               'door_number, '
                               'transmission_type, '
                               'drive_type) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;',
                               (i,
                                current_engine_code,
                                current_engine_volume,
                                current_body,
                                current_doors,
                                current_transmission,
                                current_drive,))

                inserted_specs.append(current_specs)
        print('_inserted specs')
    print('_not inserted specs, already exists')
    conn.commit()


def insert_vehicles(cursor, conn):
    print(f'...inserting vehicles')
    cursor.execute('SELECT id FROM specs')
    specs_ids = cursor.fetchall()
    print(f'...selected specs in amount of {len(specs_ids)}')

    cursor.execute('SELECT id FROM plant')
    plant_ids = cursor.fetchall()
    print(f'...selected plant ids in amount of {len(plant_ids)}')

    cursor.execute('SELECT id FROM model')
    model_ids = cursor.fetchall()
    print(f'...selected model ids in amount of {len(model_ids)}')

    list_of_values = []

    for i in range(number_of_vehicles):
        date_of_delivery = get_random_date("2000-01-01", "2021-01-01", random.random())
        current_specs = random.choice(specs_ids)
        current_model = random.choice(model_ids)
        current_color = random.choice(colors)
        current_plant = random.choice(plant_ids)
        current_date_of_manufacturing = get_random_date("2000-01-01", date_of_delivery, random.random())

        value = (
            current_specs, current_model, date_of_delivery, current_color, current_plant, current_date_of_manufacturing)
        list_of_values.append(value)

    query = ('INSERT INTO vehicle (specs_id, model, delivery_date, color, plant_id, date_of_manufacturing)'
             'VALUES  (%s, %s, %s, %s, %s, %s)')

    psycopg2.extras.execute_batch(cursor, query, list_of_values)
    print(f'_inserted ({len(list_of_values)}) vehicles')
    conn.commit()


def insert_manager(cursor, conn):
    print(f'...inserting managers')
    cursor.execute('SELECT id FROM people')
    people_ids = cursor.fetchall()
    print(f'...selected people ids in amount of {len(people_ids)}')

    cursor.execute('SELECT date_of_entry FROM people')
    people_start_dates = cursor.fetchall()
    print(f'...selected start dates in amount of {len(people_start_dates)}')

    cursor.execute('SELECT DISTINCT people_id  FROM manager')
    people_with_jobs = cursor.fetchall()
    print(f'...selected people w/jobs in amount of {len(people_with_jobs)}')

    people_ids = [x for x in people_ids if x not in people_with_jobs]
    list_of_values = []

    for people in range(len(people_ids)):

        number_of_positions = random.randint(1, 7)
        end_date = (people_start_dates[people][0]).strftime('%Y-%m-%d')

        for i in range(number_of_positions):
            start_date = end_date
            end_date = get_random_date(start_date, "2020-01-01", random.random())
            position = random.choice(
                ['sales manager', 'top salesman', 'mechanic', 'top mechanic', 'safeguard engineer'])

            # cursor.execute('INSERT INTO manager (people_id, position, start_date, end_date) VALUES (%s, %s, %s, %s);',
            #               (people_ids[people], position, start_date, end_date))

            value = (people_ids[people], position, start_date, end_date)
            list_of_values.append(value)

    query = 'INSERT INTO manager (people_id, position, start_date, end_date) VALUES (%s, %s, %s, %s);'
    psycopg2.extras.execute_batch(cursor, query, list_of_values)

    print(f'_inserted ({len(list_of_values)}) managers')
    conn.commit()


def insert_job(cursor, conn):
    amount_of_jobs = 250
    cursor.execute('SELECT COUNT(*) FROM job')
    number_of_existing_jobs = cursor.fetchone()

    if number_of_existing_jobs[0] < amount_of_jobs:
        for i in range(amount_of_jobs):
            current_price = random.randrange(1000, 200000, 500)
            current_work_description = ''.join(
                random.choice(string.ascii_uppercase + string.ascii_lowercase) for i in range(random.randint(10, 5000)))

            cursor.execute('INSERT INTO job (work_description, work_cost) VALUES (%s, %s);',
                           (current_work_description, current_price))
        print('_inserted jobs')

    print('_not inserted jobs, already exists')
    conn.commit()


def insert_maintenance(cursor, conn):
    print(f'...inserting maintenance')
    query = 'SELECT id FROM vehicle ORDER BY id DESC LIMIT ' + str(number_of_vehicles)
    cursor.execute(query)
    vehicle_ids = cursor.fetchall()
    print(f'...selected vehicle ids in amount of {len(vehicle_ids)}:')
    print(vehicle_ids)

    cursor.execute('SELECT id FROM job')
    job_ids = cursor.fetchall()
    print(f'...selected job ids in amount of {len(job_ids)}')

    query = 'SELECT date_of_manufacturing FROM vehicle ORDER BY vehicle.id DESC LIMIT ' + str(number_of_vehicles)

    cursor.execute(query)
    dates_of_manufacturing = cursor.fetchall()
    print(f'...selected dates of manufacturing in amount of {len(dates_of_manufacturing)}')

    list_of_values = []

    for car in range(len(vehicle_ids)):
        number_of_maintenances = random.randint(0, 5)
        start_date = dates_of_manufacturing[car]

        for i in range(number_of_maintenances):
            date = get_random_date(start_date[0].strftime('%Y-%m-%d'), "2022-01-01", random.random())
            current_job_id = random.choice(job_ids)

            value = (vehicle_ids[car], date, current_job_id)
            list_of_values.append(value)

    query = 'INSERT INTO maintenance (vehicle_id, date_of_visit, service_number) VALUES (%s, %s, %s)'
    psycopg2.extras.execute_batch(cursor, query, list_of_values)

    print(f'_inserted ({len(list_of_values)}) maintetances')
    conn.commit()


def insert_selling(cursor, conn):
    print(f'...inserting selling')
    query = 'SELECT delivery_date FROM vehicle ORDER BY vehicle.id LIMIT ' + str(number_of_vehicles)
    cursor.execute(query)
    delivery_dates = cursor.fetchall()
    print(f'...selected delivery dates in amount of {len(delivery_dates)}')

    cursor.execute('SELECT id FROM client')
    client_ids = cursor.fetchall()
    print(f'...selected client ids in amount of {len(client_ids)}')

    cursor.execute('SELECT id FROM manager')
    manager_ids = cursor.fetchall()
    print(f'...selected manager ids in amount of {len(manager_ids)}')

    list_of_values = []

    for car in range(len(delivery_dates)):
        min_selling_date = delivery_dates[car]

        number_of_sellings = random.randint(1, 10)
        current_vin = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(17))

        for i in range(number_of_sellings):
            current_client_id = random.choice(client_ids)
            current_seller_id = random.choice(manager_ids)
            current_selling_date = get_random_date(min_selling_date[0].strftime('%Y-%m-%d'), "2022-01-01",
                                                   random.random())

            value = (car + 1, current_client_id, current_selling_date, random.randrange(100000, 2500000, 5000),
                     random.choice(['cash', 'card']), current_seller_id, current_vin)
            list_of_values.append(value)

    query = 'INSERT INTO selling (vehicle, client_id, selling_date, subtotal, payment, seller_id, VIN)  VALUES (%s, %s, %s, %s, %s, %s, %s)'
    psycopg2.extras.execute_batch(cursor, query, list_of_values)

    print(f'_inserted ({len(list_of_values)}) sellings')
    conn.commit()


conn = connect()
cursor = initialise(conn)

init_variables()
get_surnames()
get_colors()

insert_clients(cursor, number_of_clients, conn)
insert_people(cursor, number_of_people, conn)

insert_from_file(cursor, conn, 'values/models.txt', 'INSERT INTO model (name) VALUES (%s) ON CONFLICT DO NOTHING')
insert_from_file(cursor, conn, 'values/plant_cities.txt',
                 'INSERT INTO plant_city (name) VALUES (%s) ON CONFLICT DO NOTHING')
insert_from_file(cursor, conn, 'values/body_types.txt', 'INSERT INTO body (type) VALUES (%s) ON CONFLICT DO NOTHING')

insert_plant(cursor, conn)
insert_specs(cursor, conn)
insert_vehicles(cursor, conn)
insert_manager(cursor, conn)
insert_job(cursor, conn)

insert_maintenance(cursor, conn)
insert_selling(cursor, conn)
