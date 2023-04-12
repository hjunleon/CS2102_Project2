/* Procedures */
-- CREATE EXTENSION IF NOT EXISTS "pgcrypto";
-- CREATE SEQUENCE packages_seq START 1;

CREATE OR REPLACE PROCEDURE submit_request(customer_id INTEGER, evaluator_id INTEGER, pickup_addr TEXT, pickup_postal TEXT, 
recipient_name TEXT, recipient_addr TEXT, recipient_postal TEXT, submission_time TIMESTAMP, package_num INTEGER, reported_height INTEGER[], 
reported_width INTEGER[], reported_depth INTEGER[], reported_weight INTEGER[], content TEXT[], estimated_value NUMERIC[])
AS $$
DECLARE 
	i INTEGER;
	new_request_id INTEGER;
BEGIN
	SELECT MAX(id) + 1 INTO new_request_id
	FROM delivery_requests;
	-- delivery_request_id := gen_random_uuid();
	INSERT INTO delivery_requests VALUES (new_request_id, customer_id, evaluator_id, 'submitted', pickup_addr, pickup_postal, recipient_name, recipient_addr, recipient_postal, submission_time, NULL, NULL, NULL);
	FOR i IN 1..package_num LOOP
		INSERT INTO packages VALUES(new_request_id, i, reported_height[i], reported_width[i], reported_depth[i], reported_weight[i], content[i], estimated_value[i], NULL, NULL, NULL, NULL);
	END LOOP;
END;
$$ LANGUAGE plpgsql;

--- procedure: resubmit request ---
CREATE OR REPLACE PROCEDURE resubmit_request(
	request_id INTEGER,
	evaluater_id INTEGER,
	submission_time TIMESTAMP,
	reported_height INTEGER[],
	reported_width INTEGER[],
	reported_depth INTEGER[],
	reported_weight INTEGER[]
) AS $$
DECLARE
	param_request_id INTEGER := request_id;
	curs CURSOR FOR (SELECT * FROM packages P WHERE P.request_id = param_request_id);
	d_r RECORD; -- delivery request record
	p_r RECORD; -- package record
	new_request_id integer; -- new request id
BEGIN
	-- get request record for the old information
	SELECT * INTO d_r
	FROM delivery_requests DR 
	WHERE DR.id = request_id;

	SELECT MAX(id) + 1 INTO new_request_id 
	FROM delivery_requests;

	-- insert into delivery request
	INSERT INTO delivery_requests (id, customer_id, evaluater_id, 
								   status, pickup_addr, pickup_postal, 
								   recipient_name, recipient_addr, recipient_postal, 
								   submission_time, pickup_date, num_days_needed, price)
    VALUES (new_request_id, d_r.customer_id, evaluater_id, 
			'submitted', d_r.pickup_addr, d_r.pickup_postal, 
			d_r.recipient_name, d_r.recipient_addr, d_r.recipient_postal, 
			submission_time, NULL, NULL, NULL);

	-- insert with new request id and updated attributes
	OPEN curs;
	LOOP
		FETCH curs INTO p_r;
		EXIT WHEN NOT FOUND;
		INSERT INTO packages (request_id, package_id, reported_height,
							 reported_width, reported_depth, reported_weight,
							 content, estimated_value, actual_height,
							 actual_width, actual_depth, actual_weight)
        VALUES (new_request_id, p_r.package_id, reported_height[p_r.package_id],
				reported_width[p_r.package_id], reported_depth[p_r.package_id],
				reported_weight[p_r.package_id], p_r.content, p_r.estimated_value,
				NULL, NULL, NULL, NULL);
    END LOOP;
	CLOSE curs;
END;
$$ LANGUAGE plpgsql;

-- Procedures (Q3)
CREATE OR REPLACE PROCEDURE insert_leg(
	request_id INTEGER,
	handler_id INTEGER,
    start_time TIMESTAMP,
    destination_facility INTEGER
) AS $$
DECLARE
    next_leg_id INTEGER;
	param_request_id INTEGER;
BEGIN
	param_request_id := request_id;
    SELECT COALESCE(MAX(leg_id), 0) + 1 INTO next_leg_id
    FROM legs
    WHERE legs.request_id = param_request_id;

    INSERT INTO legs (
        request_id,
        leg_id,
        handler_id,
        start_time,
        end_time,
        destination_facility
    ) VALUES (
        request_id,
        next_leg_id,
        handler_id,
        start_time,
        NULL,
        destination_facility
    );
END;
$$ LANGUAGE 'plpgsql';
