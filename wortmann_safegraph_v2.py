import psycopg2
import psycopg2.extras
import os
import csv
from csv import reader

os.system('clear')

connstring="host=localhost dbname=testdb user=pyuser password=password"



def wait_to_continue():
	input("Press enter to continue...")
	os.system('clear')



def initialize_table():
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()
	sql1 = """DROP TABLE IF EXISTS visits_info,location_info,naics_codes;

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
	"""

	cur.execute(sql1)
	conn.commit()
	cur.close()
	conn.close
	wait_to_continue()



def run_csv_import():
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()

	with open('/var/lib/postgresql/scripts/Book1.csv','r') as read_obj:
		# pass the file object to reader() to get the reader object
		csv_reader = reader(read_obj)
		header=next(csv_reader)
		for row in csv_reader:
			#print(len(row))
			if len(row)>14:
				placekey=row[0]
				location_name=row[1]
				if location_name=='':
					location_name=None
				top_category=row[2]
				if top_category=='':
					top_category=None
				sub_category=row[3]
				if sub_category=='':
					sub_category=None
				naics_code=row[4]
				if naics_code=='':
					naics_code=None
				latitude=row[5]
				if latitude=='':
					latitude=None
				longitude=row[6]
				if longitude=='':
					longitude=None
				street_address=row[7]
				if street_address=='':
					street_address=None
				city=row[8]
				if city=='':
					city=None
				region=row[9]
				if region=='':
					region=None
				postal_code=row[10]
				if postal_code=='':
					postal_code=None
				phone_number=row[11]
				if phone_number=='':
					phone_number=None
				date_range_start=row[12]
				if date_range_start=='':
					date_range_start=None
				date_range_end=row[13]
				if date_range_end=='':
					date_range_end=None
				raw_visitor_count=row[14]
				if raw_visitor_count=='':
					raw_visitor_count=None
				if placekey!=None and date_range_start!=None:
					print("Read: " + placekey.ljust(20,' ') + location_name.ljust(30,' ') + street_address.ljust(30,' ') + city.ljust(15,' ') + region.ljust(2,' ') + postal_code.ljust(6,' ') + date_range_start.ljust(10,' ') + raw_visitor_count.rjust(5,' '))
					cur.execute('CALL addrecdb2(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
					(placekey,location_name,top_category,sub_category,naics_code,latitude,longitude,street_address,
					city,region,postal_code,phone_number,date_range_start,date_range_end,raw_visitor_count));
					
	conn.commit()
	cur.close()
	conn.close
	wait_to_continue()



def run_select():
	print("Running the select routine!")
	sql="SELECT v.placekey,n.naics_code,l.latitude,l.longitude,substring(date_range_start,1,10) date_range_start,raw_visitor_count FROM visits_info v JOIN location_info l ON v.vid=l.vid AND v.placekey=l.placekey JOIN naics_codes n ON l.nid=n.nid;"
	conn=psycopg2.connect(connstring)
#	cur=conn.cursor()
	cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql)
	row=cur.fetchone();
	print("Rowcount: ", cur.rowcount)
	if row==None:
		print("No records!")
	else:
		while row is not None:
			print(row['placekey'].ljust(20,' ') + row['naics_code'].ljust(7,' ') + row['latitude'].ljust(11,' ') +
			row['longitude'].ljust(11,' ') + row['date_range_start'].ljust(10,' ') + str(row['raw_visitor_count']).ljust(5,' '))
			row=cur.fetchone()
	cur.close
	conn.close
	wait_to_continue()



def output_save_csv():
	sql='''
	SELECT l.location_name,n.top_category,n.sub_category,l.street_address,l.city,l.region,l.postal_code,substring(v.date_range_start,1,10) date_range_start,v.raw_visitor_count
	FROM location_info l
	JOIN visits_info v ON l.vid=v.vid
	JOIN naics_codes n
	on n.nid=l.nid;'''
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql)
	row=cur.fetchone();
	with open('/var/lib/postgresql/scripts/Locations_records.csv','w',newline='') as csvfile:
		fieldnames = ['Location','Top_Category','Sub_Category','Address','City','State','Zip_Code','Week_Of','Number_Visitors']
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		if row==None:
			print("No records!")
		else:
			while row is not None:
				writer.writerow({'Location':row['location_name'],'Top_Category':row['top_category'],'Sub_Category':row['sub_category'],'Address':row['street_address'],
				'City':row['city'],'State':row['region'],'Zip_Code':row['postal_code'],'Week_Of':row['date_range_start'],'Number_Visitors':row['raw_visitor_count']})
				print("Writing record... " + row['location_name'] + row['date_range_start'] + str(row['raw_visitor_count']))
				row=cur.fetchone()
			print("\nWritten record(s) to: /var/lib/postgresql/scripts/Locations_records.csv\n")
	cur.close
	conn.close
	wait_to_continue()



def output_save_txt():
	f=open('/var/lib/postgresql/scripts/Locations_records.txt','w')
	sql="SELECT l.location_name,n.top_category,n.sub_category,l.street_address,l.city,l.region,l.postal_code,substring(v.date_range_start,1,10) date_range_start,v.raw_visitor_count FROM location_info l JOIN visits_info v ON l.vid=v.vid JOIN naics_codes n on n.nid=l.nid;"
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql)
	row=cur.fetchone();
	if row==None:
		print("No records!")
	else:
		while row is not None:
			f.write(row['location_name'].ljust(30," ") + row['top_category'].ljust(63," ") + str(row['sub_category']).ljust(70," ") + row['street_address'].ljust(39," ") +
			row['city'].ljust(23," ") + row['region'].ljust(3," ") + row['postal_code'].ljust(6," ") + str(row['raw_visitor_count']).ljust(5," "))
			f.write("\n")
			print("Writing record... " + row['location_name'] + row['date_range_start'] + str(row['raw_visitor_count']))
			row=cur.fetchone()
		print("\nWritten record(s) to: /var/lib/postgresql/scripts/Locations_records.txt\n")
	cur.close
	conn.close
	wait_to_continue()



def aggregate():
#Good spot for you to do some adaptation here!
	print("aggregate module ran")
	wait_to_continue()



loop=True
while loop==True:
	print("Running Menu!")
	print("Enter 1 to select.")
	print("Enter 2 to initialize the table.")
	print("Enter 3 to import csv.")
	print("Enter 4 to output a csv.")
	print("Enter 5 to output a text file.")
#	print("Enter 6 to run an aggregate report (placeholder).")
	print("Enter 6 to quit.")
	choice=input("Enter choice: ")
	if choice=="1":
		run_select()
	elif choice=="2":
		initialize_table()
	elif choice=="3":
		run_csv_import()
	elif choice=="4":
		output_save_csv()
	elif choice=="5":
		output_save_txt()
#	elif choice=="6":
#		aggregate()
	elif choice=="6":
		loop=False;
