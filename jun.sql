-- Triggers - Unsuccessful_deliveries (Q4)

CREATE OR REPLACE FUNCTION unsuccessful_time_after_function()
RETURNS TRIGGER AS $$
DECLARE
    leg_start_time TIMESTAMP;
BEGIN
    SELECT start_time INTO leg_start_time
    FROM legs
    WHERE request_id = NEW.request_id

    IF NEW.attempt_time > leg_start_time  THEN
        RETURN NEW
    END IF

    RAISE EXCEPTION 'Cannot insert new unsuccessful_deliveries: with attempt time before leg start time.'
END;
$$ LANGUAGE 'plpgsql'

CREATE TRIGGER unsuccessful_deliveries_check1
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW EXECUTE FUNCTION unsuccessful_time_after_function()

-- Triggers - Unsuccessful_deliveries (Q5)

CREATE OR REPLACE FUNCTION max_three_fails_function()
RETURNS TRIGGER AS $$
DECLARE
    fail_counts INT;
BEGIN
    SELECT COUNT(*) INTO fail_counts
    FROM unsuccessful_deliveries
    WHERE request_id = NEW.request_id

    IF fail_counts < 3 THEN
        RETURN NEW
    END IF

    RAISE EXCEPTION 'Cannot insert new unsuccessful_deliveries: has already reached three for this delivery request.'
END;
$$ LANGUAGE 'plpgsql'

CREATE TRIGGER max_three_fails
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW EXECUTE FUNCTION max_three_fails_function()

-- Triggers - Cancelled_requests related (Q6)

CREATE OR REPLACE FUNCTION cancel_after_submit_function()
RETURNS TRIGGER AS $$
DECLARE
    submission_time TIMESTAMP;
BEGIN
    SELECT submission_time INTO submission_time
    FROM delivery_requests
    WHERE id = NEW.id

    IF NEW.cancel_time > submission_time  THEN
        RETURN NEW
    END IF

    RAISE EXCEPTION 'Cannot insert new cancelled_request: with cancel time before submission time.'
END;
$$ LANGUAGE 'plpgsql'

CREATE TRIGGER cancel_after_submit
BEFORE INSERT ON cancelled_requests
FOR EACH ROW EXECUTE FUNCTION cancel_after_submit_function()


-- Functions (Q3)
CREATE OR REPLACE FUNCTION get_top_connections(k INTEGER)
RETURNS TABLE (source_facility_id INTEGER, destination_facility_id INTEGER) AS $$
DECLARE
    top_connections CURSOR FOR (
        SELECT DISTINCT rl.source_facility as source_facility_id, l.destination_facility as  destination_facility_id,  COUNT(*) AS conn_count
        FROM legs l, return_legs rl
        WHERE l.request_id = rl.request_id AND l.leg_id = rl.leg_id
        GROUP BY l.destination_facility, rl.source_facility
        ORDER BY COUNT(*) DESC, l.destination_facility ASC, rl.source_facility ASC
        LIMIT k
    )
    conn_record RECORD
BEGIN
    OPEN top_connections
    LOOP
        FETCH top_connections INTO conn_record;
        EXIT WHEN NOT FOUND;
        RETURN NEXT;
    END LOOP;
    CLOSE top_connections;
END;
$$ LANGUAGE plpgsql;
