# История обслуживаний автомобилей
query_1 = ('SELECT vehicle_id, specs_id, m.date_of_visit, m.service_number'
           ' FROM vehicle JOIN maintenance m  on vehicle.id = m.vehicle_id GROUP BY vehicle.id, m.id;')

# Количество продаж менеджерами
query_2 = ('SELECT  p.people_name, selling.seller_id, count(*) as sellings_number, p.date_of_entry'
           ' FROM selling'
           ' JOIN manager m on selling.seller_id = m.id'
           ' JOIN people p on m.people_id = p.id'
           ' GROUP BY seller_id, m.id, people_name, date_of_entry'
           ' ORDER BY sellings_number DESC;')

# Все проданные автомобили за последний месяц
query_3 = ('SELECT * FROM selling'
           ' JOIN vehicle v on selling.vehicle = v.id'
           ' WHERE selling_date BETWEEN current_date - interval \'30 days\' AND current_date;')

# Все сервисные работы за последний год
query_4 = ('SELECT * FROM maintenance'
           ' JOIN job j on maintenance.service_number = j.id'
           ' WHERE date_of_visit BETWEEN current_date - interval \'365 days\' AND current_date;')

# Количество произведенных автомобилей некоторым заводом за все время
query_5 = ('SELECT count(*) FROM (SELECT * FROM vehicle'
           ' JOIN plant p on vehicle.plant_id = p.id'
           ' WHERE plant_id=2'
           ' GROUP BY vehicle.id, p.name, p.id) AS manufactured')

query_list = [query_1, query_2, query_3, query_4, query_5]
