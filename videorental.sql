-- This SQL code is designed to generate reports about DVD rentals with the goal of discovering the time of day that sees the most DVD rentals. The code has been split into five segments for clarity of presentation.
-- To run this code locally, first follow the instructions at https://www.postgresqltutorial.com/postgresql-getting-started/load-postgresql-sample-database/ to load the dvdrental example database on your system. The dvdrental.tar file has also been included here for convenience.
-- After that, you should be able run the script in the videorental.sql file in your program of choice.

-- Section A
--
-- Create two tables: detail_report and summary_report.
-- The detail report table is created with columns that will hold the raw data extracted from the existing rental, film and category tables.
-- The summary report table is created with columns that will hold the transformed and aggregated data.

CREATE TABLE detail_report (
	rental_id INT PRIMARY KEY,
	film_id INT,
	title VARCHAR(40),
	category VARCHAR(40),
	rating VARCHAR(10),
	rental_date TIMESTAMP,
	time_of_day VARCHAR(10),
	rental_rate NUMERIC
);

CREATE TABLE summary_report (
	time_of_day VARCHAR(10) PRIMARY KEY,
	total_rentals INT,
	total_revenue NUMERIC
);

-- Section B
--
-- Extracts the raw data from the existing tables in the DVD Rental database, and inserts the result set into the detail report table.
--  To achieve this, the query uses INNER JOINS to combine data found in the rental, film and category tables, which are given the aliases r, f, and c respectively. The rental table is the main table used in this query, and each row in the detail report corresponds to a single row in the rental table.
-- The first join extracts the relevant data from the film table, on the condition that the film id columns match. Because the rental table doesn’t include a film id column, the film id of the rental is retrieved by querying the inventory table.
-- The second join extracts the relevant data from the category table, on the condition that the category id column matches the film’s category id. Because the film table doesn’t include a category id column, the film’s category id is retrieved by querying the film category table.
-- Finally, the query filters out any results that don’t fall within the specified rental date.

INSERT INTO detail_report
SELECT
	r.rental_id,
	f.film_id,
	f.title,
	c.name AS category,
	f.rating,
	r.rental_date,
	time_of_day(r.rental_date) AS time_of_day,
	f.rental_rate
FROM rental r
INNER JOIN film f
ON f.film_id = (SELECT i.film_id FROM inventory i WHERE i.inventory_id = r.inventory_id)
INNER JOIN category c
ON c.category_id = (SELECT fc.category_id FROM film_category fc WHERE fc.film_id = f.film_id)
WHERE r.rental_date >= '2005-05-30 00:00:00'
AND r.rental_date <= '2005-05-30 23:59:59';

-- Section C
--
-- This portion of code creates the function time_of_day, that transforms a timestamp into a string to describe the time of day the rental transaction occurred. The function uses a CASE expression to check which ‘time of day’ category the rental date falls into: morning, afternoon, or evening. The function returns this result as text, which will be inserted into the summary report table.

CREATE OR REPLACE FUNCTION time_of_day(date_timestamp TIMESTAMP)
RETURNS TEXT
AS $$
DECLARE
	time_string VARCHAR;
BEGIN
	CASE
		WHEN date_timestamp::time < '12:00'
			THEN time_string := 'Morning';
		WHEN date_timestamp::time >= '12:00' AND date_timestamp::time < '17:00'
			THEN time_string := 'Afternoon';
		WHEN date_timestamp::time >= '17:00'
			THEN time_string := 'Evening';
	END CASE;
	RETURN time_string;
END $$ LANGUAGE plpgsql;

-- Section D
--
-- This section of code reates a trigger to update the summary_report table when a new row is inserted into the detail_report table.
-- The first segment of code creates the trigger function update_summary(), which inserts a new row into the summary table. The ‘time of day’ function from Section D is called here, and the return value is stored in the new entry’s time_of_day column. The summary report table should only have three rows, one row for each ‘time of day’ category. If a category already exists in the table, the ON CONFLICT statement updates the existing row by incrementing the total_rentals column and adding the new entry’s rental rate to the total_revenue column. Otherwise, the new row is inserted into the table.
-- The next segment of code creates the trigger add_to_summary, which calls the update_summary function every time a new row is added to the detail report table.

CREATE OR REPLACE FUNCTION update_summary()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
	INSERT INTO summary_report (time_of_day, total_rentals, total_revenue)
	VALUES (
		NEW.time_of_day,
		1,
		NEW.rental_rate)
	ON CONFLICT (time_of_day) DO UPDATE
		SET total_rentals = summary_report.total_rentals + 1,
			total_revenue = summary_report.total_revenue + NEW.rental_rate;
	RETURN NEW;
END;
$$;

CREATE TRIGGER add_to_summary
	AFTER INSERT
	ON detail_report
	FOR EACH ROW
	EXECUTE PROCEDURE update_summary();

-- Section E
--
-- The final section of code creates the procedure generate_reports. This procedure first clears all data and resets the id columns in the detail and summary report tables. It then uses the code from Section C to extract the data needed to generate the detail and summary reports. In this version of the code, the result set is filtered using the timestamp argument to only include rental transactions that occurred on that day.
-- To ensure all data is up-to-date, this procedure should be run at the end of each business day. To achieve this, the data administrator should create a new job in pgAgent targeting the dvdrental database. The code for this job should include a call to this procedure using the SQL standard function CURRENT_TIMESTAMP as the argument. The job should be scheduled to repeat every day at 11:59 PM eastern standard time.

CREATE OR REPLACE PROCEDURE generate_reports(_date TIMESTAMP)
LANGUAGE PLPGSQL
AS $$
BEGIN
	TRUNCATE TABLE summary_report
	RESTART IDENTITY;
	TRUNCATE TABLE detail_report
	RESTART IDENTITY;

	INSERT INTO detail_report
	SELECT
		r.rental_id,
		f.film_id,
		f.title,
		c.name AS category,
		f.rating,
		r.rental_date,
		time_of_day(r.rental_date) AS time_of_day,
		f.rental_rate
	FROM rental r
	INNER JOIN film f
	ON f.film_id = (SELECT i.film_id FROM inventory i WHERE i.inventory_id = r.inventory_id)
	INNER JOIN category c
	ON c.category_id = (SELECT fc.category_id FROM film_category fc WHERE fc.film_id = f.film_id)
	WHERE to_char(r.rental_date, 'YYYY-MM-DD') = to_char(_date, 'YYYY-MM-DD')
	AND r.rental_date <= _date;
END;
$$;
