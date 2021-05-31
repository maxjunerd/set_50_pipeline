create database stocks;

create table overall_market (
	name varchar, 
	date date,
	last float, 
	change float, 
	percent_change float, 
	high float, 
	low float, 
	volumn_k float, 
	value_m_baht float,
	ingestion_timestamp timestamp
);

create table set_50 (
	name varchar, 
	date date,
	open float, 
	high float, 
	low float, 
	last float, 
	change float, 
	percent_change float, 
	bid float, 
	offer float, 
	volumn_shares float, 
	value_k_baht float,
	ingestion_timestamp timestamp
);