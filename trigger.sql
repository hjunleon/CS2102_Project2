-- constraint 1: delivery request --
CREATE OR REPLACE FUNCTION no_package_check()
RETURNS TRIGGER AS $$
DECLARE 
	package_count integer;
BEGIN
	SELECT COUNT(*) INTO package_count
	FROM packages P
	WHERE P.request_id = NEW.id;

	IF (package_count < 1) THEN
		RAISE EXCEPTION 'Each delivery request must have at least one package';
	ELSE
		RETURN NULL;
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER no_package
AFTER INSERT ON delivery_requests
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION no_package_check();

--- constraint 2: package related ---
CREATE OR REPLACE FUNCTION sequential_package_id_check()
RETURNS TRIGGER AS $$
DECLARE
	last_id integer;
BEGIN
	SELECT COALESCE(MAX(package_id), 0) INTO last_id
	FROM packages
	WHERE request_id = NEW.request_id;

	IF (NEW.package_id <> last_id + 1) THEN
		RAISE EXCEPTION 'IDs of the packages should be consecutive integers starting from 1.';
		RETURN NULL;
	ELSE 
		RETURN NEW;
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequential_package_id
BEFORE INSERT ON packages
FOR EACH ROW EXECUTE FUNCTION sequential_package_id_check();

--- constraint 3: unsuccessful pickups related : sequential pickup_id ---
/*
Question:
- Do we assume that the pickups will be inserted into the table in order of the pickup id?
	If Yes, then cursor might be redundant, else cursor together with deferrable constraint.
*/
CREATE OR REPLACE FUNCTION sequential_pickup_id_check()
RETURNS TRIGGER AS $$
DECLARE
	last_id integer;
BEGIN
	SELECT COALESCE(MAX(pickup_id), 0) INTO last_id
	FROM unsuccessful_pickups
	WHERE request_id = NEW.request_id;

	IF (NEW.pickup_id <> last_id + 1) THEN
		RAISE EXCEPTION 'IDs of the unsuccessful pickups should be consecutive integers starting from 1.';
		RETURN NULL;
	ELSE 
		RETURN NEW;
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequential_pickup_id
BEFORE INSERT ON unsuccessful_pickups
FOR EACH ROW EXECUTE FUNCTION sequential_pickup_id_check();

--- constraint 4: unsuccessful pickups related : sequential timestamp ---
/*
Question:
- Is the pickup id inserted in sequence, hence indicate that the timestamp order will be determined by the pickup id
*/
CREATE OR REPLACE FUNCTION sequential_timestamp_check()
RETURNS TRIGGER AS $$
DECLARE 
    submission_time TIMESTAMP;
    prev_time TIMESTAMP;
BEGIN
    SELECT DR.submission_time INTO submission_time
    FROM delivery_requests DR
    WHERE id = NEW.request_id;

    IF (NEW.pickup_id = 1 AND NEW.pickup_time < submission_time) THEN
        RAISE EXCEPTION 'Unsuccessful pickup time should be after the submission_time.';
        RETURN NULL;
    END IF;

    SELECT pickup_time INTO prev_time
    FROM unsuccessful_pickups
    WHERE request_id = NEW.request_id AND pickup_id = (NEW.pickup_id - 1);

    IF (prev_time IS NOT NULL AND NEW.pickup_time < prev_time) THEN 
        RAISE EXCEPTION 'Unsuccessful pickup’s timestamp should be after the previous unsuccessful pickup’s timestamp.';
        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequential_pickup_time
BEFORE INSERT ON unsuccessful_pickups
FOR EACH ROW EXECUTE FUNCTION sequential_timestamp_check();

-- Triggers - Legs-related (Q1)
CREATE OR REPLACE FUNCTION check_leg_id_is_consecutive_function()
RETURNS TRIGGER AS $$
DECLARE
    last_leg_id INT;
BEGIN
    SELECT COALESCE(MAX(leg_id), 0) INTO last_leg_id 
    FROM legs 
    WHERE legs.request_id = NEW.request_id;

    IF (NEW.leg_id = (last_leg_id + 1)) THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'Cannot insert new leg: leg_id must be a consecutive integer starting from 1 for each delivery request.';

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_leg_id_is_consecutive
BEFORE INSERT ON legs
FOR EACH ROW EXECUTE FUNCTION check_leg_id_is_consecutive_function();


-- Triggers - Legs-related (Q2)
CREATE OR REPLACE FUNCTION check_first_leg_function()
RETURNS TRIGGER AS $$
DECLARE
    submission_time TIMESTAMP;
    last_unsuccessful_pickup TIMESTAMP;
BEGIN
    SELECT DR.submission_time INTO submission_time
    FROM delivery_requests DR
    WHERE id = NEW.request_id;

    SELECT pickup_time INTO last_unsuccessful_pickup
    FROM unsuccessful_pickups up
    WHERE request_id = NEW.request_id
    ORDER BY pickup_id DESC
    LIMIT 1;

    IF NEW.start_time > submission_time AND (last_unsuccessful_pickup IS NULL or NEW.start_time > last_unsuccessful_pickup) THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'Cannot insert new leg: with start time before submission time and last unsuccessful pickup.';
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER check_first_leg
BEFORE INSERT ON legs
FOR EACH ROW 
WHEN (NEW.leg_id = 1)
EXECUTE FUNCTION check_first_leg_function();


-- Triggers - Legs-related (Q3)
CREATE OR REPLACE FUNCTION check_is_valid_leg_function()
RETURNS TRIGGER AS $$
DECLARE
    prev_end_time TIMESTAMP;
BEGIN
    SELECT end_time INTO prev_end_time 
    FROM legs 
    WHERE legs.request_id = NEW.request_id 
    ORDER BY legs.leg_id DESC 
    LIMIT 1;

    IF (NEW.start_time < prev_end_time OR prev_end_time IS NULL) THEN
        RAISE EXCEPTION 'Cannot insert new leg: with start time before the end time of the previous leg or when the end time of the previous leg is null.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER check_is_valid_leg
BEFORE INSERT ON legs
FOR EACH ROW
WHEN (NEW.leg_id > 1)
EXECUTE FUNCTION check_is_valid_leg_function();

-- Triggers - Unsuccessful_deliveries (Q4)

CREATE OR REPLACE FUNCTION unsuccessful_time_after_function()
RETURNS TRIGGER AS $$
DECLARE
    leg_start_time TIMESTAMP;
BEGIN
    SELECT start_time INTO leg_start_time
    FROM legs
    WHERE request_id = NEW.request_id AND leg_id = NEW.leg_id;

    IF NEW.attempt_time > leg_start_time  THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'Cannot insert new unsuccessful_deliveries: with attempt time before leg start time.';
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER unsuccessful_deliveries_check1
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW EXECUTE FUNCTION unsuccessful_time_after_function();

-- Triggers - Unsuccessful_deliveries (Q5)

CREATE OR REPLACE FUNCTION max_three_fails_function()
RETURNS TRIGGER AS $$
DECLARE
    fail_counts INT;
BEGIN
    SELECT COUNT(*) INTO fail_counts
    FROM unsuccessful_deliveries
    WHERE request_id = NEW.request_id;

    IF fail_counts < 3 THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'Cannot insert new unsuccessful_deliveries: has already reached three for this delivery request.';
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER max_three_fails
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW EXECUTE FUNCTION max_three_fails_function();

-- Triggers - Cancelled_requests related (Q6)

CREATE OR REPLACE FUNCTION cancel_after_submit_function()
RETURNS TRIGGER AS $$
DECLARE
    submission_time TIMESTAMP;
BEGIN
    SELECT DR.submission_time INTO submission_time
    FROM delivery_requests DR
    WHERE id = NEW.id;

    IF NEW.cancel_time > submission_time  THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'Cannot insert new cancelled_request: with cancel time before submission time.';
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER cancel_after_submit
BEFORE INSERT ON cancelled_requests
FOR EACH ROW EXECUTE FUNCTION cancel_after_submit_function();

/* Qns 7 */
CREATE OR REPLACE FUNCTION check_return_leg_id() 
RETURNS TRIGGER
AS $$
DECLARE
	nextLegID INT;
BEGIN
	SELECT COALESCE(MAX(leg_id), 0) + 1 INTO nextLegID
	FROM return_legs
	where request_id = NEW.request_id;

	IF (NEW.leg_id <> nextLegID) THEN 
		RAISE EXCEPTION 'RETURN_LEG ID SHOULD BE SEQUENTIAL';
		RETURN NULL;
	END IF;

	RETURN NEW;
	
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_return_leg_id_trigger
BEFORE INSERT ON return_legs
FOR EACH ROW EXECUTE FUNCTION check_return_leg_id();

/* Qns 8 */
CREATE OR REPLACE FUNCTION check_return_leg() RETURNS TRIGGER
AS $$
DECLARE
	cancelTS TIMESTAMP;
	current_legs INT;
	lastlegTS TIMESTAMP;
BEGIN
	SELECT COUNT(*) INTO current_legs
	FROM legs
	WHERE NEW.request_id = request_id;

	IF (current_legs = 0) THEN 
		RAISE EXCEPTION 'No corresponding leg in delivery request';
		RETURN NULL;
	END IF;

	SELECT end_time INTO lastlegTS
	FROM legs
	WHERE NEW.request_id = request_id
	ORDER BY leg_id DESC 
	LIMIT 1;

	IF (NEW.start_time <= lastlegTS) THEN 
		RAISE EXCEPTION 'Last existing leg end time must be before the start time of the return leg';
		RETURN NULL;
	END IF;

	SELECT cancel_time INTO cancelTS
	FROM cancelled_requests
	WHERE id = NEW.request_id;

	IF (cancelTS IS NOT NULL AND cancelTS >= NEW.start_time) THEN 
		RAISE EXCEPTION 'RETURN_LEG start time should be after the cancel time of the request';
		RETURN NULL;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_return_leg_trigger
BEFORE INSERT ON return_legs
FOR EACH ROW EXECUTE FUNCTION check_return_leg();

/* Qns 9 */
CREATE OR REPLACE FUNCTION check_unsuccessful_return_deliveries() RETURNS TRIGGER
AS $$
DECLARE 
	current_count INT;
BEGIN
	SELECT COUNT(*) INTO current_count
	FROM unsuccessful_return_deliveries
	WHERE request_id = NEW.request_id;

	IF (current_count >= 3) THEN RETURN NEW;
        RAISE EXCEPTION 'There can only be at most three unsuccessful_return_deliveries';
        RETURN NULL;	
    END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_unsuccessful_return_deliveries_trigger
BEFORE INSERT ON unsuccessful_return_deliveries
FOR EACH ROW EXECUTE FUNCTION check_unsuccessful_return_deliveries();


/* Qns 10 */
CREATE OR REPLACE FUNCTION check_unsuccessful_return_deliveries_timestamp() RETURNS TRIGGER
AS $$
DECLARE 
	startTS TIMESTAMP;
BEGIN
	SELECT start_time INTO startTS
	FROM return_legs
	WHERE request_id = NEW.request_id AND leg_id = NEW.leg_id;

	IF (NEW.attempt_time < startTS) THEN 
		RAISE EXCEPTION 'Unsuccessfull return delivery timestamp should be after the start time of the corresponding return leg';
		RETURN NULL;
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER check_unsuccessful_return_deliveries_timestamp_trigger
BEFORE INSERT ON unsuccessful_return_deliveries
FOR EACH ROW EXECUTE FUNCTION check_unsuccessful_return_deliveries_timestamp();

