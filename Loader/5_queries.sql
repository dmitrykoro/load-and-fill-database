--История обслуживаний автомобилей
EXPLAIN (ANALYSE) SELECT vehicle_id, specs_id, m.date_of_visit, m.service_number FROM vehicle
JOIN maintenance m  on vehicle.id = m.vehicle_id
GROUP BY vehicle.id, m.id;

set enable_hashjoin to off;
set enable_hashjoin to on;
CREATE INDEX maintenance_vehicle ON vehicle(id, specs_id);
CREATE INDEX vehicle_maintenance ON maintenance(vehicle_id);

DROP INDEX IF EXISTS maintenance_vehicle;
DROP INDEX IF EXISTS vehicle_maintenance;

VACUUM (ANALYSE);


--Количество продаж менеджерами
EXPLAIN (ANALYSE) SELECT  p.people_name, selling.seller_id, count(*) as sellings_number, p.date_of_entry
FROM selling
JOIN manager m on selling.seller_id = m.id
JOIN people p on m.people_id = p.id
GROUP BY seller_id, m.id, people_name, date_of_entry
ORDER BY sellings_number DESC;


set enable_hashjoin to off;
set enable_hashjoin to on;
CREATE INDEX people_id ON people(id);
CREATE INDEX seller_id ON selling(seller_id);
DROP INDEX IF EXISTS people_id;
DROP INDEX IF EXISTS seller_id;


SELECT
    indexname,
    indexdef
FROM
    pg_indexes;


--Все проданные автомобили за последний месяц
EXPLAIN (ANALYSE) SELECT * FROM selling JOIN vehicle v on selling.vehicle = v.id WHERE selling_date BETWEEN current_date - interval '30 days' AND current_date;


--Все сервисные работы за последний год
EXPLAIN (ANALYSE) SELECT * FROM maintenance JOIN job j on maintenance.service_number = j.id WHERE date_of_visit BETWEEN current_date - interval '365 days' AND current_date;
CREATE INDEX maintenance_service_num ON maintenance(service_number);
DROP INDEX IF EXISTS maintenance_service_num;


--Количество произведенных автомобилей некоторым заводом за все время
EXPLAIN (ANALYSE) SELECT count(*) FROM (SELECT * FROM vehicle JOIN plant p on vehicle.plant_id = p.id WHERE plant_id=2 GROUP BY vehicle.id, p.name, p.id) AS manufactured;

set enable_seqscan  to off;
set enable_seqscan  to on;

















