DROP DATABASE IF EXISTS cs2102_project2;
CREATE DATABASE cs2102_project2;

\c cs2102_project2

CREATE TYPE gender_type AS ENUM (
	'male',
	'female'
);

CREATE TYPE delivery_status AS ENUM (
	'submitted',
	'evaluated',
	'withdrawn',
	'accepted',
	'completed',
	'cancelled',
	'unsuccessful'
);

CREATE TABLE customers (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	gender gender_type NOT NULL,
	mobile TEXT NOT NULL
);

CREATE TABLE employees (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	gender gender_type NOT NULL,
	dob DATE NOT NULL,
	title TEXT NOT NULL,
	salary NUMERIC NOT NULL,
	CHECK (salary >= 0)
);

CREATE TABLE delivery_staff (
	id INTEGER PRIMARY KEY NOT NULL REFERENCES employees(id) 
);

CREATE TABLE delivery_requests (
	id SERIAL PRIMARY KEY,
	customer_id INTEGER NOT NULL REFERENCES customers(id),
	evaluater_id INTEGER NOT NULL REFERENCES employees(id),
	status delivery_status NOT NULL,
	pickup_addr TEXT NOT NULL,
	pickup_postal TEXT NOT NULL,
	recipient_name TEXT NOT NULL,
	recipient_addr TEXT NOT NULL,
	recipient_postal TEXT NOT NULL,
	submission_time TIMESTAMP NOT NULL,
	pickup_date DATE,
	num_days_needed INTEGER,
	price NUMERIC
);

CREATE TABLE packages (
	request_id INTEGER REFERENCES delivery_requests(id),
	package_id INTEGER,
	reported_height NUMERIC NOT NULL,
	reported_width NUMERIC NOT NULL,
	reported_depth NUMERIC NOT NULL,
	reported_weight NUMERIC NOT NULL,
	content TEXT NOT NULL,
	estimated_value NUMERIC NOT NULL,
	actual_height NUMERIC,
	actual_width NUMERIC,
	actual_depth NUMERIC,
	actual_weight NUMERIC,
	PRIMARY KEY (request_id, package_id)
);

CREATE TABLE accepted_requests (
	id INTEGER PRIMARY KEY REFERENCES delivery_requests(id),
	card_number TEXT NOT NULL,
	payment_time TIMESTAMP NOT NULL,
	monitor_id INTEGER NOT NULL REFERENCES employees(id)
);

CREATE TABLE cancelled_or_unsuccessful_requests (
	id INTEGER PRIMARY KEY REFERENCES accepted_requests(id)
);

CREATE TABLE cancelled_requests (
	id INTEGER PRIMARY KEY REFERENCES accepted_requests(id),
	cancel_time TIMESTAMP NOT NULL
);

CREATE TABLE unsuccessful_pickups (
	request_id INTEGER REFERENCES accepted_requests(id),
	pickup_id INTEGER,
	handler_id INTEGER NOT NULL REFERENCES delivery_staff(id),
	pickup_time TIMESTAMP NOT NULL,
	reason TEXT,
	PRIMARY KEY (request_id, pickup_id)
);

CREATE TABLE facilities (
	id SERIAL PRIMARY KEY,
	address TEXT NOT NULL,
	postal TEXT NOT NULL
);
	
CREATE TABLE legs (
	request_id INTEGER REFERENCES accepted_requests(id),
	leg_id INTEGER,
	handler_id INTEGER NOT NULL REFERENCES delivery_staff(id),	
    start_time TIMESTAMP NOT NULL,
	end_time TIMESTAMP,
	destination_facility INTEGER REFERENCES facilities(id),
	PRIMARY KEY (request_id, leg_id)
);

CREATE TABLE unsuccessful_deliveries (
    request_id INTEGER,
    leg_id INTEGER,
    reason TEXT NOT NULL,
    attempt_time TIMESTAMP NOT NULL,
	PRIMARY KEY (request_id, leg_id),
    FOREIGN KEY (request_id, leg_id) REFERENCES legs(request_id, leg_id)
);

CREATE TABLE return_legs (
	request_id INTEGER REFERENCES cancelled_or_unsuccessful_requests(id),
	leg_id INTEGER,
	handler_id INTEGER NOT NULL REFERENCES delivery_staff(id),	
    start_time TIMESTAMP NOT NULL,
    source_facility INTEGER NOT NULL REFERENCES facilities(id),
	end_time TIMESTAMP,
	PRIMARY KEY (request_id, leg_id)
);

CREATE TABLE unsuccessful_return_deliveries (
    request_id INTEGER,
    leg_id INTEGER,
    reason TEXT NOT NULL,
    attempt_time TIMESTAMP NOT NULL,
	PRIMARY KEY (request_id, leg_id), 
    FOREIGN KEY (request_id, leg_id) REFERENCES return_legs(request_id, leg_id)
);

\c postgres
