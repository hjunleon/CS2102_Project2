
-- Triggers - Legs-related (Q1)
CREATE OR REPLACE FUNCTION inser_consecutive_legs_function()
RETURNS TRIGGER AS $$
DECLARE
    last_leg_id INT;
BEGIN
    SELECT COALESCE(MAX(leg_id), 0) INTO last_leg_id 
    FROM legs 
    WHERE legs.request_id = NEW.request_id 

    IF (NEW.leg_id = (last_leg_id + 1)) THEN
        RETURN NEW
    END IF

    RAISE EXCEPTION 'Cannot insert new leg: leg_id must be a consecutive integer starting from 1 for each delivery request.'
END;
$$ LANGUAGE 'plpgsql'

CREATE TRIGGER inser_consecutive_legs
BEFORE INSERT ON delivery_requests
FOR EACH ROW EXECUTE FUNCTION inser_consecutive_legs_function()


-- Triggers - Legs-related (Q2)
CREATE OR REPLACE FUNCTION first_leg_insertion_function()
RETURNS TRIGGER AS $$
DECLARE
    submission_time TIMESTAMP;
    last_unsuccessful_pickup TIMESTAMP;
BEGIN
    SELECT submission_time INTO submission_time
    FROM delivery_requests
    WHERE request_id = NEW.request_id

    SELECT pickup_time INTO last_unsuccessful_pickup
    FROM unsuccessful_pickups up
    WHERE request_id = NEW.request_id
    ORDER BY pickup_id DESC
    LIMIT 1;

    IF NEW.start_time > submission_time AND (last_unsuccessful_pickup IS NULL or NEW.start_time > last_unsuccessful_pickup) THEN
        RETURN NEW
    END IF

    RAISE EXCEPTION 'Cannot insert new leg: with start time before submission time and last unsuccessful pickup.'
END;
$$ LANGUAGE 'plpgsql'

CREATE TRIGGER first_leg_insertion
BEFORE INSERT ON delivery_requests
FOR EACH ROW EXECUTE FUNCTION first_leg_insertion_function()


-- Triggers - Legs-related (Q3)
CREATE OR REPLACE FUNCTION new_leg_insertion_function()
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
        RAISE EXCEPTION 'Cannot insert new leg: with start time before the end time of the previous leg or when the end time of the previous leg is null.'
    END IF

    RETURN NEW
END;
$$ LANGUAGE 'plpgsql'

CREATE TRIGGER new_leg_insertion
BEFORE INSERT ON delivery_requests
FOR EACH ROW EXECUTE FUNCTION new_leg_insertion_function()

-- Procedures (Q3)
CREATE OR REPLACE PROCEDURE insert_leg(
	request_id INTEGER,
	handler_id INTEGER,
    start_time TIMESTAMP,
    destination_facility INTEGER
) AS $$
DECLARE
    next_leg_id INTEGER
BEGIN
    SELECT COALESCE(MAX(leg_id) + 1, 1) INTO next_leg_id
    FROM legs
    WHERE legs.request_id = request_id

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


-- Functions (Q2)
CREATE OR REPLACE FUNCTION get_top_delivery_persons (k INTEGER)
RETURNS TABLE (employee_id INTEGER) AS $$
DECLARE
    top_persons CURSOR FOR (
        SELECT handler_id
        FROM
            SELECT handler_id, SUM(count)
            FROM
                SELECT handler_id, COUNT(handler_id)
                FROM legs
                GROUP BY handler_id
                UNION ALL
                SELECT handler_id, COUNT(handler_id)
                FROM return_legs
                GROUP BY handler_id
                UNION ALL
                SELECT handler_id, COUNT(handler_id)
                FROM unsuccessful_pickups
                GROUP BY handler_id
            GROUP BY handler_id
        ORDER BY sum DESC
        LIMIT k
    )
    person_record RECORD
BEGIN
    OPEN top_persons
    LOOP
        FETCH top_persons INTO person_record;
        EXIT WHEN NOT FOUND;
        RETURN NEXT;
    END LOOP;
    CLOSE top_persons;
END;
$$ LANGUAGE plpgsql;