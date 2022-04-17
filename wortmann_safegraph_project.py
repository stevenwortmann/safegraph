# Steve Wortmann
import psycopg2
import psycopg2.extras
import os
os.system('clear')

connstring="host=localhost dbname=testdb user=pyuser password=password"




def wait_to_continue():
	input("Press enter to continue...")
	os.system('clear')




def initialize_table():
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()
	sql1 = """DROP TABLE IF EXISTS locations CASCADE;
		  CREATE TABLE locations (
		  locid SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		  placekey VARCHAR(50) NOT NULL,
		  parent_placekey VARCHAR(50) NOT NULL,
		  location_name VARCHAR(50) NOT NULL,
		  safegraph_brand_ids VARCHAR(100) NOT NULL, --array of strings
		  brandsVARCHAR(100) NOT NULL, --array of strings
		  top_category VARCHAR(100) NOT NULL,
		  sub_category VARCHAR(100) NOT NULL,
		  naics_code INTEGER,
		  latitude NUMERIC,
		  longitude NUMERIC,
		  street_address VARCHAR(100) NOT NULL,
		  city VARCHAR(100) NOT NULL,
		  region VARCHAR(100) NOT NULL,
		  postal_code VARCHAR(100) NOT NULL,
		  iso_country_code VARCHAR(2) NOT NULL,
		  phone_number VARCHAR(100) NOT NULL,
		  open_hours VARCHAR(100) NOT NULL,
		  category_tags VARCHAR(100) NOT NULL, --array of strings
		  opened_on VARCHAR(100) NOT NULL,
		  closed_on VARCHAR(100) NOT NULL,
		  tracking_closed_since VARCHAR(100) NOT NULL,
		  geometry_type VARCHAR(100) NOT NULL,
		  date_range_start TIMESTAMP,
		  date_range_end TIMESTAMP,
		  raw_visit_counts INTEGER,
		  raw_visitor_counts INTEGER,
		  visits_by_day JSON NOT NULL,
		  visits_by_each_hour JSON NOT NULL,
		  poi_cbg VARCHAR(12) NOT NULL,
		  visitor_home_cbgs JSON NOT NULL,
		  visitor_home_aggregation JSON NOT NULL,
		  visitor_daytime_cbgs JSON NOT NULL,
		  visitor_country_of_origin JSON NOT NULL,
		  distance_from_home INTEGER,
		  median_dwell NUMERIC,
		  bucketed_dwell_times JSON NOT NULL,
		  related_same_day_brand JSON NOT NULL,
		  related_same_week_brand JSON NOT NULL,
		  device_type JSON NOT NULL,
		  normalized_visits_by_state_scaling NUMERIC,
		  normalized_visits_by_region_naics_visits NUMERIC,
		  normalized_visits_by_region_naics_visitors NUMERIC,
		  normalized_visits_by_total_visits NUMERIC,
		  normalized_visits_by_total_visitors NUMERIC
		  );"""
	cur.execute(sql1) # update
	sql2="""INSERT INTO locations(location_name,latitude,longitude,street_address,city,region,postal_code)
			VALUES ('Taco Bell','41.15403','-81.347299','805 E Main St','Kent','OH','44240');
		INSERT INTO locations(location_name,latitude,longitude,street_address,city,region,postal_code)
			VALUES ('Dollar General','39.040579','-76.058104','545 Railroad Ave','Centreville','MD','21617');
		INSERT INTO locations(location_name,latitude,longitude,street_address,city,region,postal_code)
			VALUES ('Walmart Supercenter','38.237498','-77.509135','10001 Southpoint Pkwy','Fredericksburg','VA','22407');
		INSERT INTO locations(location_name,latitude,longitude,street_address,city,region,postal_code)
			VALUES ('Rivers Casino','40.447336','-80.022281','777 Casino Dr','Pittsburgh','PA','15212');
		INSERT INTO locations(location_name,latitude,longitude,street_address,city,region,postal_code)
			VALUES ('Chick-fil-A','34.530615','-82.634469','1641 E Greenville St','Anderson','SC','29621');
		INSERT INTO locations(location_name,latitude,longitude,street_address,city,region,postal_code)
			VALUES ('Wawa','36.657829','-76.224899','101 Hillcrest Pkwy','Chesapeake','VA','23322');"""
	cur.execute(sql2)
	cur.close()
	conn.commit()
	conn.close
	wait_to_continue()




def run_select():
	print("Running the select routine...")
	sql="SELECT * FROM locations;"
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql)
	print("Rowcount: ", cur.rowcount)
	row=cur.fetchone();
	if row==None:
		print("No records!")
	else:
		while row is not None:
			print(str(row['locid'])+", "+row['location_name']+", "+row['latitude']+", "+row['longitude']+
			", "+ row['street_address']+", "+row['city']+", "+row['region']+", "+row['postal_code'])
			row=cur.fetchone()
	cur.close
	conn.close
	wait_to_continue()

def search_record():
	print("Running the search routine...")
	searchtext=input("Please enter the id of a location you'd like to search for: ")
	sql="SELECT * FROM locations WHERE locid=%s"
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql,(searchtext))
	row=cur.fetchone();
	if row==None:
		print("No records!")
	else:
		while row is not None:
			print(str(row['locid'])+", "+row['location_name']+", "+row['latitude']+", "+row['longitude']+
			", "+ row['street_address']+", "+row['city']+", "+row['region']+", "+row['postal_code'])
			row=cur.fetchone()
	cur.close
	conn.close
	wait_to_continue()

# Start here...

def insert_record():
	print("Running the insert routine...")
	loc=input("Enter location name: ")
	lat=input("Enter latitude: ")
	lon=input("Enter longitude: ")
	street=input("Enter street address: ")
	city=input("Enter city name: ")
	region=input("Enter state: \n1 for OH\n2 for PA\n3 for MD\n4 for DC\n5 for VA\n6 for NC\n7 for SC\n")
	if region==1:
		region="OH"
	elif region==2:
		region="PA"
	elif region==3:
		region="MD"
	elif region==4:
		region="DC"
	elif region==5:
		region="VA"
	elif region==6:
		region="NC"
	elif region==7:
		region="SC"
	zi=input("Enter postal code: ")
	sql = """INSERT INTO locations(location_name,latitude,longitude,street_address,city,region,postal_code)
             VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING locid;"""
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()
	cur.execute(sql,(loc,lat,lon,street,city,region,zi))
	lid=cur.fetchone()[0]
	conn.commit()
	cur.close
	conn.close
	print("ID generated was " + str(lid))
	wait_to_continue()




def delete_record():
	print("Running the delete routine...")
	sql="SELECT * FROM locations;"
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql)
	row=cur.fetchone();
	if row==None:
		print("No records!")
	else:
		while row is not None:
			print("ID:"+str(row['locid'])+", "+row['location_name']+", "+row['latitude']+", "+row['longitude']+
			", "+ row['street_address']+", "+row['city']+", "+row['region']+", "+row['postal_code'])
			row=cur.fetchone()
	locid=input("Please identify the Loc ID of the record you'd like to delete:")
	sql="DELETE FROM locations WHERE locid=%s;"
	cur.execute(sql, (locid))
	conn.commit()
	cur.close
	conn.close
	wait_to_continue()





def update_record():
	print("Running the update routine...")
	sql="SELECT * FROM locations;"
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql)
	row=cur.fetchone();
	if row==None:
		print("No records!")
	else:
		while row is not None:
			print(str(row['locid'])+", "+row['location_name']+", "+row['latitude']+", "+row['longitude']+
			", "+ row['street_address']+", "+row['city']+", "+row['region']+", "+row['postal_code'])
			row=cur.fetchone()
	locid=input("Please identify the Loc ID of the record you'd like to update:")
	sql="SELECT * FROM locations where locid=%s"
	cur.execute(sql,(locid))
	row2=cur.fetchone();
	if row2==None:
		print("No records!")
	else:
		while row2 is not None:
			oldloc=row2['location_name']
			oldlat=row2['latitude']
			oldlong=row2['longitude']
			oldstreet=row2['street_address']
			oldcity=row2['city']
			oldreg=row2['region']
			oldzip=row2['postal_code']
			row2=cur.fetchone();
	newloc=input("Old location name:     " + oldloc + "     . New name? ")
	newlat=input("Old latitude value:     " + str(oldlat) + "     . New latitude value? ")
	newlong=input("Old longitude value:     " + str(oldlong) + "     . New longitude value? ")
	newstreet=input("Old street address:     " + oldstreet + "     . New street address? ")
	newcity=input("Old city name:     " + oldstreet + "     . New city name? ")
	newreg=input("Old state:     " + oldreg + "     . New state? \n1 for OH\n2 for PA\n3 for MD\n4 for DC\n5 for VA\n6 for NC\n7 for SC\n")
	if newreg==1:
		newreg="OH"
	elif newreg==2:
		newreg="PA"
	elif newreg==3:
		newreg="MD"
	elif newreg==4:
		newreg="DC"
	elif newreg==5:
		newreg="VA"
	elif newreg==6:
		newreg="NC"
	elif newreg==7:
		newreg="SC"
	newzip=input("Old zip code:     " + oldzip + "     . New zip code? ")
	sql="UPDATE locations SET location_name=%s, latitude=%s, longitude=%s, street_address=%s, city=%s, region=%s, postal_code=%s where locid=%s;"
	cur.execute(sql,(newloc,newlat,newlong,newstreet,newcity,newreg,newzip,locid))
	conn.commit()
	cur.close
	conn.close
	wait_to_continue()



#main program loop starts here
loop=True
while loop==True:
	print("Running Menu...")
	print("Enter 1 to select.")
	print("Enter 2 to initialize the table.")
	print("Enter 3 to insert.")
	print("Enter 4 to update.")
	print("Enter 5 to delete.")
	print("Enter 6 to search by location ID.")
	print("Enter 7 to quit.")
	choice=input("Enter choice:")
	if choice=="1":
		run_select()
	elif choice=="2":
		initialize_table()
	elif choice=="3":
		insert_record()
	elif choice=="4":
		update_record()
	elif choice=="5":
		delete_record()
	elif choice=="6":
		search_record()
	elif choice=="7":
		loop=False;
#end loop
