
DROP DATABASE IF EXISTS otus;
CREATE DATABASE otus;



CREATE TABLE pickup_by_hour (
hour_of_day           INT,
total_trips           INT
);

CREATE TABLE distance_distribution (
trip_distance           INT,
count           INT
);

