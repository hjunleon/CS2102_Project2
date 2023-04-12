/* Functions */
CREATE OR REPLACE FUNCTION view_trajectory(request_id INTEGER)
RETURNS TABLE (source_addr TEXT, destination_addr TEXT, start_time TIMESTAMP, end_time TIMESTAMP) AS $$
DECLARE 
	param_request_id INTEGER := request_id;
	curs CURSOR FOR (SELECT * FROM legs JOIN delivery_requests ON legs.request_id = delivery_requests.id AND legs.request_id = param_request_id ORDER BY legs.leg_id ASC);
	curs2 CURSOR FOR (SELECT * FROM return_legs JOIN delivery_requests ON return_legs.request_id = delivery_requests.id AND return_legs.request_id = param_request_id ORDER BY return_legs.leg_id DESC);
	r RECORD;
	r2 RECORD;
	prv_dest TEXT;
	ahead_src TEXT;
BEGIN

	CREATE TEMP TABLE temp_data (source_addr TEXT, destination_addr TEXT, start_time TIMESTAMP, end_time TIMESTAMP) ON COMMIT DROP;

	prv_dest := NULL;
	OPEN curs;
	LOOP
		FETCH curs INTO r;
		EXIT WHEN NOT FOUND;
		IF r.destination_facility IS NULL THEN destination_addr := r.recipient_addr;
		ELSE destination_addr := (SELECT F.address FROM facilities F WHERE F.id = r.destination_facility);
		END IF;
		start_time := r.start_time;
		end_time := r.end_time;
		IF prv_dest IS NULL THEN source_addr := r.pickup_addr;
		ELSE source_addr := prv_dest;
		END IF;
		INSERT INTO temp_data VALUES (source_addr, destination_addr, start_time, end_time);
		prv_dest := (SELECT F.address FROM facilities F WHERE F.id = r.destination_facility);
	END LOOP;
	CLOSE curs;

	/* THIS IS FOR RETURN LEGS */
	OPEN curs2;
	ahead_src := NULL;
	LOOP
		FETCH curs2 INTO r2;
		EXIT WHEN NOT FOUND;
		source_addr := (SELECT F.address FROM facilities F where F.id = r2.source_facility);
		start_time := r2.start_time;
		end_time := r2.end_time;
		IF ahead_src IS NULL THEN destination_addr := r2.pickup_addr;
		ELSE destination_addr := ahead_src;
		END IF;
		INSERT INTO temp_data VALUES (source_addr, destination_addr, start_time, end_time);
		ahead_src := (SELECT F.address FROM facilities F where F.id = r2.source_facility);
	END LOOP;
	CLOSE curs2;

	/* Time to sort them based on ascending start_time */
	RETURN QUERY SELECT * FROM temp_data ORDER BY start_time ASC;
END;
$$ LANGUAGE plpgsql;

-- Functions (Q2)
CREATE OR REPLACE FUNCTION get_top_delivery_persons (k INTEGER)
RETURNS TABLE (employee_id INTEGER) AS $$
BEGIN
    RETURN QUERY
    WITH
        trips AS (
            SELECT handler_id, COUNT(*) as trip_count
            FROM (
                SELECT handler_id FROM legs
                UNION ALL
                SELECT handler_id FROM return_legs
                UNION ALL
                SELECT handler_id FROM unsuccessful_pickups
            ) AS a
            GROUP BY handler_id
        )

    SELECT id
    FROM delivery_staff ds FULL OUTER JOIN trips
    ON ds.id = trips.handler_id
    GROUP BY id, trip_count
    ORDER BY trip_count DESC, id ASC
    LIMIT k;
END;
$$ LANGUAGE plpgsql;

-- Functions (Q3)
CREATE OR REPLACE FUNCTION get_top_connections(k Integer)
RETURNS TABLE(source_facility_id Integer, destination_facility_id Integer)
AS $$
BEGIN
    RETURN QUERY
    WITH trips AS (
        SELECT L1.destination_facility AS source_facility, L2.destination_facility AS destination_facility
        FROM legs L1, legs L2
        WHERE L1.request_id = L2.request_id
		AND L2.destination_facility IS NOT NULL
        AND  L1.leg_id + 1 = L2.leg_id
        UNION ALL
        SELECT R1.source_facility AS source_facility, R2.source_facility AS destination_facility
        FROM return_legs R1, return_legs R2
        WHERE R1.request_id = R2.request_id
		AND R2.source_facility IS NOT NULL
        AND  R1.leg_id + 1 = R2.leg_id
    ), 
    trip_occurrences AS (
        SELECT source_facility, destination_facility, COUNT(*) AS occurences
        FROM trips
		GROUP BY source_facility, destination_facility
		ORDER BY occurences DESC, source_facility, destination_facility ASC
    )
    SELECT source_facility AS source_facility_id, destination_facility AS destination_facility_id
    FROM trip_occurrences
    LIMIT k;
END;
$$ LANGUAGE plpgsql;
