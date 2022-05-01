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
	sql1 = """DROP TABLE IF EXISTS visits_info,location_info,brands_info,naics_codes;

	CREATE TABLE naics_codes (
		nid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		top_category VARCHAR(100) NOT NULL,
		sub_category VARCHAR(100),
		naics_code VARCHAR(20)
		);

	CREATE TABLE brands_info (
		bid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		safegraph_brand_ids VARCHAR(100) NOT NULL,
		brands VARCHAR(100),
		nid INT REFERENCES naics_codes (nid) ON DELETE CASCADE
		);

	CREATE TABLE visits_info (
		vid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		placekey VARCHAR(20) NOT NULL,
		date_range_start VARCHAR(30) NOT NULL,
		date_range_end VARCHAR(30) NOT NULL,
		raw_visit_counts INT,
		raw_visitor_counts INT,
		visits_by_day VARCHAR(100),
		visits_by_each_hour VARCHAR(1000),
		visitor_home_cbgs VARCHAR(1000),
		visitor_home_aggregation VARCHAR(1000),
		visitor_daytime_cbgs VARCHAR(1000),
		visitor_country_of_origin VARCHAR(100),
		distance_from_home INT,
		median_dwell INT,
		bucketed_dwell_times VARCHAR(1000),
		related_same_day_brand VARCHAR(1000),
		related_same_week_brand VARCHAR(1000),
		device_type VARCHAR(100),
		normalized_visits_by_state_scaling FLOAT,
		normalized_visits_by_region_naics_visits FLOAT,
		normalized_visits_by_region_naics_visitors FLOAT,
		normalized_visits_by_total_visits FLOAT,
		normalized_visits_by_total_visitors FLOAT
		);

	CREATE TABLE location_info (
		locid INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		placekey VARCHAR(20) NOT NULL,
		parent_placekey VARCHAR(20),
		location_name VARCHAR(100) NOT NULL,
		vid INT REFERENCES visits_info (vid) ON DELETE CASCADE,
		nid INT REFERENCES naics_codes (nid) ON DELETE CASCADE,
		bid INT REFERENCES brands_info (bid) ON DELETE CASCADE,
		latitude VARCHAR(15) NOT NULL,
		longitude VARCHAR(15) NOT NULL,
		street_address VARCHAR(100) NOT NULL,
		city VARCHAR(50) NOT NULL,
		region VARCHAR(5) NOT NULL,
		postal_code VARCHAR(10) NOT NULL,
		iso_country_code VARCHAR(10),
		phone_number VARCHAR(20),
		open_hours VARCHAR(1000),
		category_tags VARCHAR(1000),
		opened_on VARCHAR(10),
		closed_on VARCHAR(10),
		tracking_closed_since VARCHAR(10),
		geometry_type VARCHAR(10),
		poi_cbg VARCHAR(20)
		);

	DROP PROCEDURE IF EXISTS addrecdb2;

	CREATE PROCEDURE addrecdb2(
		a_pk VARCHAR(20),
		b_ppk VARCHAR(20),
		c_lo VARCHAR(100),
		d_sbid VARCHAR(100),
		e_bds VARCHAR(100),
		f_tc VARCHAR(100),
		g_sc VARCHAR(100),
		h_nc VARCHAR(20),
		i_lt VARCHAR(15),
		j_lg VARCHAR(15),
		k_sa VARCHAR(100),
		l_ci VARCHAR(50),
		m_rg VARCHAR(5),
		n_pc VARCHAR(10),
		o_cy VARCHAR(10),
		p_pn VARCHAR(20),
		q_op VARCHAR(1000),
		r_ct VARCHAR(1000),
		s_oo VARCHAR(10),
		t_co VARCHAR(10),
		u_ts VARCHAR(10),
		v_gt VARCHAR(10),
		w_ds VARCHAR(30),
		x_de VARCHAR(30),
		y_rvt INT,
		z_rvr INT,
		aa_vbd VARCHAR(100),
		ab_vbh VARCHAR(1000),
		ac_cbg VARCHAR(20),
		ad_vhc VARCHAR(1000),
		ae_vha VARCHAR(1000),
		af_vdc VARCHAR(1000),
		ag_vco VARCHAR(100),
		ah_dfh INT,
		ai_md INT,
		aj_bdt VARCHAR(1000),
		ak_rsd VARCHAR(1000),
		al_rsw VARCHAR(1000),
		am_dt VARCHAR(100),
		an_nvss FLOAT,
		ao_nvrnt FLOAT,
		ap_nvnvr FLOAT,
		aq_nvtvt FLOAT,
		ar_nvtv FLOAT
	)
	AS $$
	DECLARE locidout INT;
	DECLARE bidout INT;
	DECLARE nidout INT;
	DECLARE vidout INT;

	BEGIN

		IF (SELECT COUNT(*) FROM visits_info WHERE (placekey=a_pk AND date_range_start=w_ds))=1 THEN
				SELECT vid INTO vidout FROM visits_info WHERE (placekey=a_pk AND date_range_start=w_ds);
			ELSE
				INSERT INTO visits_info(placekey,date_range_start,date_range_end,raw_visit_counts,
										raw_visitor_counts,visits_by_day,visits_by_each_hour,visitor_home_cbgs,
										visitor_home_aggregation,visitor_daytime_cbgs,visitor_country_of_origin,
										distance_from_home,median_dwell,bucketed_dwell_times,related_same_day_brand,
										related_same_week_brand,device_type,normalized_visits_by_state_scaling,
										normalized_visits_by_region_naics_visits,normalized_visits_by_region_naics_visitors,
										normalized_visits_by_total_visits,normalized_visits_by_total_visitors)
				VALUES (a_pk,w_ds,x_de,y_rvt,z_rvr,aa_vbd,ab_vbh,ad_vhc,ae_vha,af_vdc,ag_vco,ah_dfh,ai_md,aj_bdt,ak_rsd,
						al_rsw,am_dt,an_nvss,ao_nvrnt,ap_nvnvr,aq_nvtvt,ar_nvtv);
				SELECT LASTVAL() INTO vidout;
			END IF;

		IF (SELECT COUNT(*) FROM naics_codes WHERE naics_code=h_nc)=1 THEN
				SELECT nid INTO nidout FROM naics_codes WHERE naics_code=nc;
			ELSE
				INSERT INTO naics_codes(top_category,sub_category,naics_code)
				VALUES (f_tc,g_sc,h_nc);
				SELECT LASTVAL() INTO nidout;
			END IF;

		IF (SELECT COUNT(*) FROM brands_info WHERE safegraph_brand_ids=d_sbid)=1 THEN
				SELECT bid INTO bidout FROM naics_codes WHERE safegraph_brand_ids=d_sbid;
			ELSE
				INSERT INTO brands_info(safegraph_brand_ids,brands,nid)
				VALUES (d_sbid,e_bds,nidout);
				SELECT LASTVAL() INTO bidout;
			END IF;

		IF (SELECT COUNT(*) FROM location_info WHERE placekey=a_pk)=1 THEN
				SELECT locid INTO locidout FROM location_info WHERE placekey=a_pk;
			ELSE
				INSERT INTO location_info(placekey,parent_placekey,location_name,vid,nid,bid,latitude,longitude,
					street_address,city,region,postal_code,iso_country_code,phone_number,open_hours,category_tags,
					opened_on,closed_on,tracking_closed_since,geometry_type,poi_cbg)
				VALUES (a_pk,b_ppk,c_lo,vidout,nidout,bidout,i_lt,j_lg,k_sa,l_ci,m_rg,n_pc,
						o_cy,p_pn,q_op,r_ct,s_oo,t_co,u_ts,v_gt,ac_cbg);
				SELECT LASTVAL() INTO locidout;
			END IF;

	END; $$
	language plpgsql;

	call addrecdb2('224-223@63c-rjh-pd9','','La Cabra Craft Tacos','','','Restaurants and Other Eating Places','Full-Service Restaurants',
	'722511','39.955599','-82.004667','1335 Linden Ave','Zanesville','OH','43701','US','17402978132',
	'','Brunch,Mexican Food','','',
	'2019-07','POLYGON','2022-04-18T00:00:00-04:00','2022-04-25T00:00:00-04:00','115','75','[13,16,19,13,17,24,13]',
	'[0,0,0,0,0,0,0,0,0,1,0,0,0,4,0,0,3,0,1,1,3,0,0,0,1,0,0,0,0,0,0,0,0,3,0,2,1,1,1,1,1,1,0,2,2,0,0,0,1,1,0,0,0,0,0,0,0,1,1,0,0,1,1,2,4,3,2,0,2,0,0,0,2,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,4,0,1,1,1,2,0,1,0,0,0,0,0,0,0,0,2,1,1,1,0,0,0,2,1,7,0,0,1,0,0,0,2,0,0,0,0,0,0,0,0,1,3,3,0,1,1,1,1,3,1,5,1,1,0,0,0,0,0,0,0,0,0,0,0,1,1,1,2,4,2,0,0,1,0,0,0,1,0]','391199118004',
	'{"390897574002":5,"391130005005":4,"391199127003":4,"391199126001":4,"391199116004":4,"391199116003":4,"391199112003":4,"391199111003":4,"391199111002":4,"391279662003":4,"390450329004":4,"390830072002":4,"391199128002":4,"470930058082":4,"391199123001":4,"391199127002":4,"390897589006":4,"391199117001":4,"391199127001":4}',
	'{"39119911700":7,"39119912100":5,"47093005808":4,"39119912400":4,"39119911500":4,"39089757400":4,"39119911300":4,"39119912000":4,"39089754101":4,"39119911200":4,"39119912300":4,"39119912800":4,"39083006801":4,"39119911100":4,"39089758900":4,"39119911600":4,"39119912700":4}',
	'{"391199112004":5,"391130005005":4,"391199128002":4,"390830068013":4,"390830072002":4,"391199111003":4,"390897589006":4,"391199117001":4,"390450329004":4,"391199116003":4,"391199117003":4,"391199115004":4,"470930058082":4,"391199124003":4,"391199127001":4}',
	'{"US":72}','9064','45','{"<5":1,"5-10":22,"11-20":3,"21-60":43,"61-120":23,"121-240":4,">240":19}',
	'{"Country Fair":16,"McDonald's":14,"Walmart":12,"Taco Bell":9,"Duchess":8,"Tim Hortons":8,"BP":6,"Wendy's":6,"KFC":5,"Speedway":5,"Dairy Queen":5,"Kroger":4,"The Home Depot":4,"GameStop":4,"Marathon":4,"Subway":4,"Smoker Friendly":3,"Lowe's":3,"Starbucks":3,"Sheetz":3}',
	'{"Walmart":40,"McDonald's":39,"Country Fair":32,"Kroger":28,"BP":25,"Sheetz":23,"Subway":23,"Duchess":21,"Taco Bell":17,"Speedway":16,"Dollar General":16,"Dairy Queen":16,"Wendy's":12,"CVS":12,"Tim Hortons":12,"Marathon":12,"KFC":11,"Arby's":11,"Smoker Friendly":9,"Lowe's":9}',
	'{"android":27,"ios":47}','1319.3152211025','0.0000580570','0.0000730162','0.0000066229','0.0000154705');

	call addrecdb2('222-222@63d-kqz-49z','','Burger King','SG_BRAND_60d8d6d29e2c4b14f4ea1983baefd36e','Burger King','Restaurants and Other Eating Places','Limited-Service Restaurants',
	'722513','40.295395','-78.836567','440 Galleria Dr','Johnstown','PA','15904','US','18142627551',
	'{ "Mon": [["6:00", "22:00"]], "Tue": [["6:00", "22:00"]], "Wed": [["6:00", "22:00"]], "Thu": [["6:00", "22:00"]], "Fri": [["6:00", "22:00"]], "Sat": [["6:00", "22:00"]], "Sun": [["6:00", "22:00"]] }','Counter Service,Late Night,Lunch,Fast Food,Drive Through,Breakfast,Dinner,Burgers','','',
	'2019-07','POLYGON','2022-04-18T00:00:00-04:00','2022-04-25T00:00:00-04:00','98','83','[5,6,15,13,19,22,18]',
	'[0,0,0,0,0,0,0,0,0,1,0,0,0,2,0,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,1,1,1,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,2,1,3,3,2,1,0,2,1,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,2,1,2,0,1,0,3,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,3,3,1,1,4,0,1,1,2,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,4,6,3,0,3,0,1,0,2,2,0,0,0,0,0,0,0,0,1,2,1,2,0,0,1,3,4,0,1,3,0,0,0,0,0,0]','420210107001',
	'{"420210105002":6,"420210133002":5,"420210125003":4,"420210113003":4,"421110201024":4,"420210113001":4,"340130028001":4,"420210129004":4,"420210114004":4,"420210101004":4,"420210107002":4,"420210102002":4,"420210133001":4,"420210110001":4,"420210127001":4,"420210108013":4,"420210101003":4,"420210106001":4,"420210103001":4}',
	'{"42021013300":9,"42021010300":8,"42021010700":7,"42021011900":4,"42021010500":4,"42021013200":4,"42021010100":4,"42021010801":4,"42021010600":4,"42111020200":4,"42021000700":4,"42021013600":4,"42111020400":4,"42111020102":4,"42021013100":4,"42021013500":4}',
	'{"420210107001":5,"420210133004":4,"420210132004":4,"420210136002":4,"420210129002":4,"420210101002":4,"421110203003":4,"420210131001":4,"420210103001":4,"420210133003":4,"420210002002":4,"420210125003":4,"420210114004":4,"420210133002":4,"420210136003":4,"420210135001":4,"420210120005":4,"340130028001":4,"420210137001":4,"421110215001":4,"420210105002":4,"421110204001":4,"420210007003":4}',
	'{"US":83}','7756','11','{"<5":7,"5-10":39,"11-20":26,"21-60":17,"61-120":2,"121-240":5,">240":2}',
	'{"Sheetz":30,"Walmart":10,"ALDI":10,"Taco Bell":9,"Cricket Wireless":8,"Dollar General":6,"Rite Aid":5,"AT&T":4,"T.J. Maxx":3,"Primanti Bros.":3,"Dairy Queen":3,"McDonald's":3,"Subway":3,"Dollar Tree":3,"Kawasaki Motors":2,"Ross Stores":2,"KFC":2,"Starbucks":2,"Big Lots Stores":2,"United States Postal Service (USPS)":2}',
	'{"Sheetz":66,"Walmart":45,"McDonald's":30,"Dollar General":29,"Dairy Queen":16,"Sunoco":16,"Subway":16,"ALDI":14,"Cricket Wireless":14,"Rite Aid":13,"Family Dollar Stores":13,"Taco Bell":12,"T.J. Maxx":11,"Dollar Tree":11,"AT&T":8,"Perkins Restaurant & Bakery":8,"Arby's":8,"IGA":7,"Primanti Bros.":7,"Wendy's":7}',
	'{"android":40,"ios":41}','1289.8365329770','0.0000671073','0.0000870675','0.0000066811','0.0000185553');

	SELECT 'location_info',count(*) as 'Record Count' FROM FROM location_info UNION ALL
	SELECT 'brands_info',count(*) as 'Record Count' FROM brands_info UNION ALL
	SELECT 'naics_codes',count(*) as 'Record Count' FROM naics_codes UNION ALL
	SELECT 'visits_info',count(*) as 'Record Count' FROM visits_info;
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
