DROP TABLE IF EXISTS visitsInfo,locationInfo,brandsInfo,naicsCodes;


CREATE TABLE naicsCodes (
	nid INT IDENTITY() AS IDENTITY PRIMARY KEY,
	top_category VARCHAR(100) NOT NULL,
	sub_category VARCHAR(100),
	naics_code VARCHAR(20)
	);


CREATE TABLE brandsInfo (
	bid INT IDENTITY() AS IDENTITY PRIMARY KEY,
	safegraph_brand_ids VARCHAR(100),
	brands VARCHAR(100),
	nid INT REFERENCES naicsCodes (nid) ON DELETE CASCADE
	);


CREATE TABLE visitsInfo (
	vid INT IDENTITY() AS IDENTITY PRIMARY KEY,
	placekey VARCHAR(20) NOT NULL,
	date_range_start VARCHAR(30) NOT NULL,
	date_range_end VARCHAR(30) NOT NULL,
	raw_visit_counts INT,
	raw_visitor_counts INT,
	visits_by_day VARCHAR(100),
	visits_by_each_hour VARCHAR(10000),
	visitor_home_cbgs VARCHAR(10000),
	visitor_home_aggregation VARCHAR(10000),
	visitor_daytime_cbgs VARCHAR(10000),
	visitor_country_of_origin VARCHAR(100),
	distance_from_home INT,
	median_dwell FLOAT,
	bucketed_dwell_times VARCHAR(10000),
	related_same_day_brand VARCHAR(10000),
	related_same_week_brand VARCHAR(10000),
	device_type VARCHAR(100),
	normalized_visits_by_state_scaling FLOAT,
	normalized_visits_by_region_naics_visits FLOAT,
	normalized_visits_by_region_naics_visitors FLOAT,
	normalized_visits_by_total_visits FLOAT,
	normalized_visits_by_total_visitors FLOAT
	);


CREATE TABLE locationInfo (
	locid INT IDENTITY() AS IDENTITY PRIMARY KEY,
	placekey VARCHAR(20) NOT NULL,
	parent_placekey VARCHAR(20),
	location_name VARCHAR(100) NOT NULL,
	vid INT REFERENCES visitsInfo (vid) ON DELETE CASCADE,
	nid INT REFERENCES dbo.naicsCodes (nid) ON DELETE CASCADE,
	bid INT REFERENCES dbo.brandsInfo (bid) ON DELETE CASCADE,
	latitude VARCHAR(15) NOT NULL,
	longitude VARCHAR(15) NOT NULL,
	naics_code VARCHAR(100) NOT NULL,
	city VARCHAR(50) NOT NULL,
	region VARCHAR(5) NOT NULL,
	street_address VARCHAR(10) NOT NULL,
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

DROP PROCEDURE IF EXISTS addSGrec;


CREATE PROCEDURE addSGrec(
	@a_pk VARCHAR(20),
	@b_ppk VARCHAR(20),
	@c_lo VARCHAR(100),
	@d_sbid VARCHAR(100),
	@e_bds VARCHAR(100),
	@f_tc VARCHAR(100),
	@g_sc VARCHAR(100),
	@h_nc VARCHAR(20),
	@i_lt VARCHAR(15),
	@j_lg VARCHAR(15),
	@k_sa VARCHAR(100),
	@l_ci VARCHAR(50),
	@m_rg VARCHAR(5),
	@n_pc VARCHAR(10),
	@o_cy VARCHAR(10),
	@p_pn VARCHAR(20),
	@q_op VARCHAR(1000),
	@r_ct VARCHAR(1000),
	@s_oo VARCHAR(10),
	@t_co VARCHAR(10),
	@u_ts VARCHAR(10),
	@v_gt VARCHAR(10),
	@w_ds VARCHAR(30),
	@x_de VARCHAR(30),
	@y_rvt INT,
	@z_rvr INT,
	@aa_vbd VARCHAR(100),
	@ab_vbh VARCHAR(10000),
	@ac_cbg VARCHAR(20),
	@ad_vhc VARCHAR(10000),
	@ae_vha VARCHAR(10000),
	@af_vdc VARCHAR(10000),
	@ag_vco VARCHAR(100),
	@ah_dfh INT,
	@ai_md FLOAT,
	@aj_bdt VARCHAR(10000),
	@ak_rsd VARCHAR(10000),
	@al_rsw VARCHAR(10000),
	@am_dt VARCHAR(100),
	@an_nvss FLOAT,
	@ao_nvrnt FLOAT,
	@ap_nvnvr FLOAT,
	@aq_nvtvt FLOAT,
	@ar_nvtv FLOAT
)
AS $$
DECLARE @locidout INT;
DECLARE @bidout INT;
DECLARE @nidout INT;
DECLARE @vidout INT;

BEGIN

	IF (SELECT COUNT(*) FROM visitsInfo WHERE (placekey=a_pk AND date_range_start=w_ds))=1 BEGIN

			SELECT @vidout = vid FROM visitsInfo WHERE (placekey=a_pk AND date_range_start=w_ds);
		END
		ELSE BEGIN
			INSERT INTO visitsInfo(placekey,date_range_start,date_range_end,raw_visit_counts,
									raw_visitor_counts,visits_by_day,visits_by_each_hour,visitor_home_cbgs,
									visitor_home_aggregation,visitor_daytime_cbgs,visitor_country_of_origin,
									distance_from_home,median_dwell,bucketed_dwell_times,related_same_day_brand,
									related_same_week_brand,device_type,normalized_visits_by_state_scaling,
									normalized_visits_by_region_naics_visits,normalized_visits_by_region_naics_visitors,
									normalized_visits_by_total_visits,normalized_visits_by_total_visitors)
			VALUES (a_pk,w_ds,x_de,y_rvt,z_rvr,aa_vbd,ab_vbh,ad_vhc,ae_vha,af_vdc,ag_vco,ah_dfh,ai_md,aj_bdt,ak_rsd,
					al_rsw,am_dt,an_nvss,ao_nvrnt,ap_nvnvr,aq_nvtvt,ar_nvtv);

			SELECT @vidout = LASTVAL();
		END

	IF (SELECT COUNT(*) FROM naicsCodes WHERE naics_code=h_nc)=1 BEGIN

			SELECT @nidout = nid FROM naicsCodes WHERE naics_code=h_nc;
		END
		ELSE BEGIN
			INSERT INTO naicsCodes(top_category,sub_category,naics_code)
			VALUES (f_tc,g_sc,h_nc);

			SELECT @nidout = LASTVAL();
		END

	IF (SELECT COUNT(*) FROM brandsInfo WHERE safegraph_brand_ids=d_sbid)=1 BEGIN

			SELECT @bidout = bid FROM brandsInfo WHERE safegraph_brand_ids=d_sbid;
		END
		ELSE BEGIN
			INSERT INTO brandsInfo(safegraph_brand_ids,brands,nid)
			VALUES (d_sbid,e_bds,@nidout);

			SELECT @bidout = LASTVAL();
		END

	IF (SELECT COUNT(*) FROM locationInfo WHERE placekey=a_pk)=1 BEGIN

			SELECT @locidout = locid FROM locationInfo WHERE placekey=a_pk;
		END
		ELSE BEGIN
			INSERT INTO locationInfo(placekey,parent_placekey,location_name,vid,nid,bid,latitude,longitude,
				naics_code,city,region,street_address,iso_country_code,phone_number,open_hours,category_tags,
				opened_on,closed_on,tracking_closed_since,geometry_type,poi_cbg)
			VALUES (a_pk,b_ppk,c_lo,@vidout,@nidout,@bidout,i_lt,j_lg,k_sa,l_ci,m_rg,n_pc,
					o_cy,p_pn,q_op,r_ct,s_oo,t_co,u_ts,v_gt,ac_cbg);

			SELECT @locidout = LASTVAL();
		END

END; $$
language plpgsql;

call addSGrec('224-223@63c-rjh-pd9','','La Cabra Craft Tacos','','','Restaurants and Other Eating Places','Full-Service Restaurants',
'722511','39.955599','-82.004667','1335 Linden Ave','Zanesville','OH','43701','US','17402978132',
'','Brunch,Mexican Food','','',
'2019-07','POLYGON','2022-04-18T00:00:00-04:00','2022-04-25T00:00:00-04:00','115','75','[13,16,19,13,17,24,13]',
'[0,0,0,0,0,0,0,0,0,1,0,0,0,4,0,0,3,0,1,1,3,0,0,0,1,0,0,0,0,0,0,0,0,3,0,2,1,1,1,1,1,1,0,2,2,0,0,0,1,1,0,0,0,0,0,0,0,1,1,0,0,1,1,2,4,3,2,0,2,0,0,0,2,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,4,0,1,1,1,2,0,1,0,0,0,0,0,0,0,0,2,1,1,1,0,0,0,2,1,7,0,0,1,0,0,0,2,0,0,0,0,0,0,0,0,1,3,3,0,1,1,1,1,3,1,5,1,1,0,0,0,0,0,0,0,0,0,0,0,1,1,1,2,4,2,0,0,1,0,0,0,1,0]','391199118004',
'{"390897574002":5,"391130005005":4,"391199127003":4,"391199126001":4,"391199116004":4,"391199116003":4,"391199112003":4,"391199111003":4,"391199111002":4,"391279662003":4,"390450329004":4,"390830072002":4,"391199128002":4,"470930058082":4,"391199123001":4,"391199127002":4,"390897589006":4,"391199117001":4,"391199127001":4}',
'{"39119911700":7,"39119912100":5,"47093005808":4,"39119912400":4,"39119911500":4,"39089757400":4,"39119911300":4,"39119912000":4,"39089754101":4,"39119911200":4,"39119912300":4,"39119912800":4,"39083006801":4,"39119911100":4,"39089758900":4,"39119911600":4,"39119912700":4}',
'{"391199112004":5,"391130005005":4,"391199128002":4,"390830068013":4,"390830072002":4,"391199111003":4,"390897589006":4,"391199117001":4,"390450329004":4,"391199116003":4,"391199117003":4,"391199115004":4,"470930058082":4,"391199124003":4,"391199127001":4}',
'{"US":72}','9064','45','{"<5":1,"5-10":22,"11-20":3,"21-60":43,"61-120":23,"121-240":4,">240":19}',
'{"Country Fair":16,"McDonalds":14,"Walmart":12,"Taco Bell":9,"Duchess":8,"Tim Hortons":8,"BP":6,"Wendys":6,"KFC":5,"Speedway":5,"Dairy Queen":5,"Kroger":4,"The Home Depot":4,"GameStop":4,"Marathon":4,"Subway":4,"Smoker Friendly":3,"Lowes":3,"Starbucks":3,"Sheetz":3}',
'{"Walmart":40,"McDonalds":39,"Country Fair":32,"Kroger":28,"BP":25,"Sheetz":23,"Subway":23,"Duchess":21,"Taco Bell":17,"Speedway":16,"Dollar General":16,"Dairy Queen":16,"Wendys":12,"CVS":12,"Tim Hortons":12,"Marathon":12,"KFC":11,"Arbys":11,"Smoker Friendly":9,"Lowes":9}',
'{"android":27,"ios":47}','1319.3152211025','0.0000580570','0.0000730162','0.0000066229','0.0000154705');

call addSGrec('222-222@63d-kqz-49z','','Burger King','SG_BRAND_60d8d6d29e2c4b14f4ea1983baefd36e','Burger King','Restaurants and Other Eating Places','Limited-Service Restaurants',
'722513','40.295395','-78.836567','440 Galleria Dr','Johnstown','PA','15904','US','18142627551',
'{ "Mon": [["6:00", "22:00"]], "Tue": [["6:00", "22:00"]], "Wed": [["6:00", "22:00"]], "Thu": [["6:00", "22:00"]], "Fri": [["6:00", "22:00"]], "Sat": [["6:00", "22:00"]], "Sun": [["6:00", "22:00"]] }','Counter Service,Late Night,Lunch,Fast Food,Drive Through,Breakfast,Dinner,Burgers','','',
'2019-07','POLYGON','2022-04-18T00:00:00-04:00','2022-04-25T00:00:00-04:00','98','83','[5,6,15,13,19,22,18]',
'[0,0,0,0,0,0,0,0,0,1,0,0,0,2,0,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,1,1,1,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,2,1,3,3,2,1,0,2,1,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,2,1,2,0,1,0,3,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,3,3,1,1,4,0,1,1,2,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,4,6,3,0,3,0,1,0,2,2,0,0,0,0,0,0,0,0,1,2,1,2,0,0,1,3,4,0,1,3,0,0,0,0,0,0]','420210107001',
'{"420210105002":6,"420210133002":5,"420210125003":4,"420210113003":4,"421110201024":4,"420210113001":4,"340130028001":4,"420210129004":4,"420210114004":4,"420210101004":4,"420210107002":4,"420210102002":4,"420210133001":4,"420210110001":4,"420210127001":4,"420210108013":4,"420210101003":4,"420210106001":4,"420210103001":4}',
'{"42021013300":9,"42021010300":8,"42021010700":7,"42021011900":4,"42021010500":4,"42021013200":4,"42021010100":4,"42021010801":4,"42021010600":4,"42111020200":4,"42021000700":4,"42021013600":4,"42111020400":4,"42111020102":4,"42021013100":4,"42021013500":4}',
'{"420210107001":5,"420210133004":4,"420210132004":4,"420210136002":4,"420210129002":4,"420210101002":4,"421110203003":4,"420210131001":4,"420210103001":4,"420210133003":4,"420210002002":4,"420210125003":4,"420210114004":4,"420210133002":4,"420210136003":4,"420210135001":4,"420210120005":4,"340130028001":4,"420210137001":4,"421110215001":4,"420210105002":4,"421110204001":4,"420210007003":4}',
'{"US":83}','7756','11','{"<5":7,"5-10":39,"11-20":26,"21-60":17,"61-120":2,"121-240":5,">240":2}',
'{"Sheetz":30,"Walmart":10,"ALDI":10,"Taco Bell":9,"Cricket Wireless":8,"Dollar General":6,"Rite Aid":5,"AT&T":4,"T.J. Maxx":3,"Primanti Bros.":3,"Dairy Queen":3,"McDonalds":3,"Subway":3,"Dollar Tree":3,"Kawasaki Motors":2,"Ross Stores":2,"KFC":2,"Starbucks":2,"Big Lots Stores":2,"United States Postal Service (USPS)":2}',
'{"Sheetz":66,"Walmart":45,"McDonalds":30,"Dollar General":29,"Dairy Queen":16,"Sunoco":16,"Subway":16,"ALDI":14,"Cricket Wireless":14,"Rite Aid":13,"Family Dollar Stores":13,"Taco Bell":12,"T.J. Maxx":11,"Dollar Tree":11,"AT&T":8,"Perkins Restaurant & Bakery":8,"Arbys":8,"IGA":7,"Primanti Bros.":7,"Wendys":7}',
'{"android":40,"ios":41}','1289.8365329770','0.0000671073','0.0000870675','0.0000066811','0.0000185553');


SELECT 'locationInfo',count(*) as Record_Count FROM locationInfo UNION ALL
SELECT 'brandsInfo',count(*) as Record_Count FROM brandsInfo UNION ALL
SELECT 'naicsCodes',count(*) as Record_Count FROM naicsCodes UNION ALL
SELECT 'visitsInfo',count(*) as Record_Count FROM visitsInfo;
