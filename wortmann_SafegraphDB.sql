DROP TABLE IF EXISTS visits_info,location_info,naics_codes;

	CREATE TABLE naics_codes (
		nid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		top_category VARCHAR(100) NOT NULL,
		sub_category VARCHAR(100),
		naics_code VARCHAR(20)
		);

	CREATE TABLE visits_info (
		vid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		placekey VARCHAR(20) NOT NULL,
		date_range_start VARCHAR(30) NOT NULL,
		date_range_end VARCHAR(30) NOT NULL,
		raw_visitor_count INT
		);

	CREATE TABLE location_info (
		locid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		placekey VARCHAR(20) NOT NULL,
		location_name VARCHAR(100) NOT NULL,
		vid INT REFERENCES visits_info (vid) ON DELETE CASCADE,
		nid INT REFERENCES naics_codes (nid) ON DELETE CASCADE,
		latitude VARCHAR(15) NOT NULL,
		longitude VARCHAR(15) NOT NULL,
		street_address VARCHAR(100) NOT NULL,
		city VARCHAR(50) NOT NULL,
		region VARCHAR(5) NOT NULL,
		postal_code VARCHAR(10) NOT NULL,
		phone_number VARCHAR(20)
		);

	DROP PROCEDURE IF EXISTS addrecdb2;
	CREATE PROCEDURE addrecdb2(
		pk VARCHAR(20),
		lo VARCHAR(100),
		tc VARCHAR(100),
		sc VARCHAR(100),
		nc VARCHAR(20),
		lt VARCHAR(15),
		lg VARCHAR(15),
		sa VARCHAR(100),
		ci VARCHAR(50),
		rg VARCHAR(5),
		pc VARCHAR(10),
		pn VARCHAR(20),
		ds VARCHAR(30),
		de VARCHAR(30),
		ct INT
	)
	AS $$
	DECLARE locidout INT;
	DECLARE nidout INT;
	DECLARE vidout INT;

	BEGIN

		IF (SELECT COUNT(*) FROM visits_info WHERE (placekey=pk AND date_range_start=ds))=1 THEN
				SELECT vid INTO vidout FROM visits_info WHERE (placekey=pk AND date_range_start=ds);
			ELSE
				INSERT INTO visits_info(placekey,date_range_start,date_range_end,raw_visitor_count)
				VALUES (pk,ds,de,ct);
				SELECT LASTVAL() INTO vidout;
			END IF;

		IF (SELECT COUNT(*) FROM naics_codes WHERE naics_code=nc)=1 THEN
				SELECT nid INTO nidout FROM naics_codes WHERE naics_code=nc;
			ELSE
				INSERT INTO naics_codes(top_category,sub_category,naics_code)
				VALUES (tc,sc,nc);
				SELECT LASTVAL() INTO nidout;
			END IF;

		IF (SELECT COUNT(*) FROM location_info WHERE placekey=pk)=1 THEN
				SELECT locid INTO locidout FROM location_info WHERE placekey=pk;
			ELSE
				INSERT INTO location_info(placekey,location_name,vid,nid,latitude,
					longitude,street_address,city,region,postal_code,phone_number)
				VALUES (pk,lo,vidout,nidout,lt,lg,sa,ci,rg,pc,pn);
				SELECT LASTVAL() INTO locidout;
			END IF;

	END; $$
	language plpgsql;

	call addrecdb2('testkey1','CCAC','Educashun','Skool','922110','41.443','-81.975','1500 5th Ave',
		'Salina','KS','11111','17177769377','2022-03-21T00:00:00-04:00','2022-03-28T00:00:00-04:00','224');
	call addrecdb2('testkey2','Burgertown','Restaurants','Full-Service','722511','39.504','-79.849','5901 Lancaster Ave',
		'Wichita','KS','22222','18036286800','2022-03-21T00:00:00-04:00','2022-03-28T00:00:00-04:00','600');
		
	SELECT * FROM location_info;
	SELECT * FROM naics_codes;
	SELECT * FROM visits_info;
