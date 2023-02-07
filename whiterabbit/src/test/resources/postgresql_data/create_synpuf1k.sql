/* This is a stripped (only the tables needed are created) of: */

/*********************************************************************************
# Copyright 2014-6 Observational Health Data Sciences and Informatics
#
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/

/************************

 ####### #     # ####### ######      #####  ######  #     #           #######      #####     
 #     # ##   ## #     # #     #    #     # #     # ##   ##    #    # #           #     #    
 #     # # # # # #     # #     #    #       #     # # # # #    #    # #                 #    
 #     # #  #  # #     # ######     #       #     # #  #  #    #    # ######       #####     
 #     # #     # #     # #          #       #     # #     #    #    #       # ### #          
 #     # #     # #     # #          #     # #     # #     #     #  #  #     # ### #          
 ####### #     # ####### #           #####  ######  #     #      ##    #####  ### #######  
                                                                              

script to create OMOP common data model, version 5.2 for PostgreSQL database

last revised: 14 July 2017

Authors:  Patrick Ryan, Christian Reich


*************************/


/**************************

Standardized meta-data

***************************/


CREATE TABLE cdm_source
(
    cdm_source_name                        VARCHAR(255)    NOT NULL,
    cdm_source_abbreviation                VARCHAR(25)     NULL,
    cdm_holder                             VARCHAR(255)    NULL,
    source_description                     TEXT            NULL,
    source_documentation_reference         VARCHAR(255)    NULL,
    cdm_etl_reference                      VARCHAR(255)    NULL,
    source_release_date                    DATE            NULL,
    cdm_release_date                       DATE            NULL,
    cdm_version                            VARCHAR(10)     NULL,
    vocabulary_version                     VARCHAR(20)     NULL
)
;







/************************

Standardized clinical data

************************/


CREATE TABLE person 
    (
     person_id						INTEGER		NOT NULL , 
     gender_concept_id				INTEGER		NOT NULL , 
     year_of_birth					INTEGER		NOT NULL , 
     month_of_birth					INTEGER		NULL, 
     day_of_birth					INTEGER		NULL, 
	 birth_datetime					TIMESTAMP 	NULL,
     race_concept_id				INTEGER		NOT NULL, 
     ethnicity_concept_id			INTEGER		NOT NULL, 
     location_id					INTEGER		NULL, 
     provider_id					INTEGER		NULL, 
     care_site_id					INTEGER		NULL, 
     person_source_value			VARCHAR(50) NULL, 
     gender_source_value			VARCHAR(50) NULL,
	 gender_source_concept_id		INTEGER		NULL, 
     race_source_value				VARCHAR(50) NULL, 
	 race_source_concept_id			INTEGER		NULL, 
     ethnicity_source_value			VARCHAR(50) NULL,
	 ethnicity_source_concept_id	INTEGER		NULL
    ) 
;





CREATE TABLE observation_period 
    ( 
     observation_period_id				INTEGER		NOT NULL , 
     person_id							INTEGER		NOT NULL , 
     observation_period_start_date		DATE		NOT NULL , 
     observation_period_end_date		DATE		NOT NULL ,
	 period_type_concept_id				INTEGER		NOT NULL
    ) 
;



CREATE TABLE death
    ( 
     person_id							INTEGER			NOT NULL , 
     death_date							DATE			NOT NULL , 
     death_datetime							TIMESTAMP			NULL ,
     death_type_concept_id				INTEGER			NOT NULL ,
     cause_concept_id					INTEGER			NULL , 
     cause_source_value					VARCHAR(50)		NULL,
	 cause_source_concept_id			INTEGER			NULL
    ) 
;



CREATE TABLE visit_occurrence 
    ( 
     visit_occurrence_id			INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     visit_concept_id				INTEGER			NOT NULL , 
	 visit_start_date				DATE			NOT NULL , 
	 visit_start_datetime			TIMESTAMP		NULL ,
     visit_end_date					DATE			NOT NULL ,
	 visit_end_datetime				TIMESTAMP		NULL ,
	 visit_type_concept_id			INTEGER			NOT NULL ,
	 provider_id					INTEGER			NULL,
     care_site_id					INTEGER			NULL, 
     visit_source_value				VARCHAR(50)		NULL,
	 visit_source_concept_id		INTEGER			NULL ,
	 admitting_source_concept_id	INTEGER			NULL ,
	 admitting_source_value			VARCHAR(50)		NULL ,
	 discharge_to_concept_id		INTEGER		NULL ,
	 discharge_to_source_value		VARCHAR(50)		NULL ,
	 preceding_visit_occurrence_id	INTEGER			NULL
    ) 
;



CREATE TABLE procedure_occurrence 
    ( 
     procedure_occurrence_id		INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     procedure_concept_id			INTEGER			NOT NULL , 
     procedure_date					DATE			NOT NULL , 
     procedure_datetime					TIMESTAMP			NOT NULL ,
     procedure_type_concept_id		INTEGER			NOT NULL ,
	 modifier_concept_id			INTEGER			NULL ,
	 quantity						INTEGER			NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			INTEGER			NULL , 
     procedure_source_value			VARCHAR(50)		NULL ,
	 procedure_source_concept_id	INTEGER			NULL ,
	 qualifier_source_value			VARCHAR(50)		NULL
    ) 
;



CREATE TABLE drug_exposure 
    ( 
     drug_exposure_id				INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     drug_concept_id				INTEGER			NOT NULL , 
     drug_exposure_start_date		DATE			NOT NULL , 
     drug_exposure_start_datetime	TIMESTAMP		NOT NULL ,
     drug_exposure_end_date			DATE			NOT NULL ,
     drug_exposure_end_datetime		TIMESTAMP		NULL ,
     verbatim_end_date				DATE			NULL ,
	 drug_type_concept_id			INTEGER			NOT NULL ,
     stop_reason					VARCHAR(20)		NULL , 
     refills						INTEGER			NULL , 
     quantity						NUMERIC			NULL , 
     days_supply					INTEGER			NULL , 
     sig							TEXT	NULL , 
	 route_concept_id				INTEGER			NULL ,
	 lot_number						VARCHAR(50)		NULL ,
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			INTEGER			NULL , 
     drug_source_value				VARCHAR(50)		NULL ,
	 drug_source_concept_id			INTEGER			NULL ,
	 route_source_value				VARCHAR(50)		NULL ,
	 dose_unit_source_value			VARCHAR(50)		NULL
    ) 
;


CREATE TABLE device_exposure 
    ( 
     device_exposure_id				INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     device_concept_id				INTEGER			NOT NULL , 
     device_exposure_start_date		DATE			NOT NULL , 
     device_exposure_start_datetime		TIMESTAMP			NOT NULL ,
     device_exposure_end_date		DATE			NULL ,
     device_exposure_end_datetime		TIMESTAMP			NULL ,
     device_type_concept_id			INTEGER			NOT NULL ,
	 unique_device_id				VARCHAR(50)		NULL ,
	 quantity						INTEGER			NULL ,
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			INTEGER			NULL , 
     device_source_value			VARCHAR(100)	NULL ,
	 device_source_concept_id		INTEGER			NULL
    ) 
;


CREATE TABLE condition_occurrence 
    ( 
     condition_occurrence_id		INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     condition_concept_id			INTEGER			NOT NULL , 
     condition_start_date			DATE			NOT NULL , 
     condition_start_datetime			TIMESTAMP			NOT NULL ,
     condition_end_date				DATE			NULL ,
     condition_end_datetime				TIMESTAMP			NULL ,
     condition_type_concept_id		INTEGER			NOT NULL ,
     stop_reason					VARCHAR(20)		NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			INTEGER			NULL , 
     condition_source_value			VARCHAR(50)		NULL ,
	 condition_source_concept_id	INTEGER			NULL ,
	 condition_status_source_value	VARCHAR(50)		NULL ,
     condition_status_concept_id	INTEGER			NULL 
    ) 
;



CREATE TABLE measurement 
    ( 
     measurement_id					INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     measurement_concept_id			INTEGER			NOT NULL , 
     measurement_date				DATE			NOT NULL , 
     measurement_datetime				TIMESTAMP		NULL ,
	 measurement_type_concept_id	INTEGER			NOT NULL ,
	 operator_concept_id			INTEGER			NULL , 
     value_as_number				NUMERIC			NULL , 
     value_as_concept_id			INTEGER			NULL , 
     unit_concept_id				INTEGER			NULL , 
     range_low						NUMERIC			NULL , 
     range_high						NUMERIC			NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			INTEGER			NULL ,  
     measurement_source_value		VARCHAR(50)		NULL , 
	 measurement_source_concept_id	INTEGER			NULL ,
     unit_source_value				VARCHAR(50)		NULL ,
	 value_source_value				VARCHAR(50)		NULL
    ) 
;



CREATE TABLE observation
    ( 
     observation_id					INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     observation_concept_id			INTEGER			NOT NULL , 
     observation_date				DATE			NOT NULL , 
     observation_datetime				TIMESTAMP		NULL ,
     observation_type_concept_id	INTEGER			NOT NULL , 
	 value_as_number				NUMERIC			NULL , 
     value_as_string				VARCHAR(60)		NULL , 
     value_as_concept_id			INTEGER			NULL , 
	 qualifier_concept_id			INTEGER			NULL ,
     unit_concept_id				INTEGER			NULL , 
     provider_id					INTEGER			NULL , 
     visit_occurrence_id			INTEGER			NULL , 
     observation_source_value		VARCHAR(50)		NULL ,
	 observation_source_concept_id	INTEGER			NULL , 
     unit_source_value				VARCHAR(50)		NULL ,
	 qualifier_source_value			VARCHAR(50)		NULL
    ) 
;



/************************

Standardized health system data

************************/



CREATE TABLE location 
    ( 
     location_id					INTEGER			NOT NULL , 
     address_1						VARCHAR(50)		NULL , 
     address_2						VARCHAR(50)		NULL , 
     city							VARCHAR(50)		NULL , 
     state							VARCHAR(2)		NULL , 
     zip							VARCHAR(9)		NULL , 
     county							VARCHAR(20)		NULL , 
     location_source_value			VARCHAR(50)		NULL
    ) 
;



CREATE TABLE care_site 
    ( 
     care_site_id						INTEGER			NOT NULL , 
	 care_site_name						VARCHAR(255)	NULL ,
     place_of_service_concept_id		INTEGER			NULL ,
     location_id						INTEGER			NULL , 
     care_site_source_value				VARCHAR(50)		NULL , 
     place_of_service_source_value		VARCHAR(50)		NULL
    ) 
;


	
CREATE TABLE provider 
    ( 
     provider_id					INTEGER			NOT NULL ,
	 provider_name					VARCHAR(255)	NULL , 
     NPI							VARCHAR(20)		NULL , 
     DEA							VARCHAR(20)		NULL , 
     specialty_concept_id			INTEGER			NULL , 
     care_site_id					INTEGER			NULL , 
	 year_of_birth					INTEGER			NULL ,
	 gender_concept_id				INTEGER			NULL ,
     provider_source_value			VARCHAR(50)		NULL , 
     specialty_source_value			VARCHAR(50)		NULL ,
	 specialty_source_concept_id	INTEGER			NULL , 
	 gender_source_value			VARCHAR(50)		NULL ,
	 gender_source_concept_id		INTEGER			NULL
    ) 
;




/************************

Standardized health economics

************************/


CREATE TABLE payer_plan_period 
    ( 
     payer_plan_period_id			INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     payer_plan_period_start_date	DATE			NOT NULL , 
     payer_plan_period_end_date		DATE			NOT NULL , 
     payer_source_value				VARCHAR (50)	NULL , 
     plan_source_value				VARCHAR (50)	NULL , 
     family_source_value			VARCHAR (50)	NULL 
    ) 
;




CREATE TABLE cost 
    (
     cost_id					INTEGER	    NOT NULL , 
     cost_event_id       INTEGER     NOT NULL ,
     cost_domain_id       VARCHAR(20)    NOT NULL ,
     cost_type_concept_id       INTEGER     NOT NULL ,
     currency_concept_id			INTEGER			NULL ,
     total_charge						NUMERIC			NULL , 
     total_cost						NUMERIC			NULL , 
     total_paid						NUMERIC			NULL , 
     paid_by_payer					NUMERIC			NULL , 
     paid_by_patient						NUMERIC			NULL , 
     paid_patient_copay						NUMERIC			NULL , 
     paid_patient_coinsurance				NUMERIC			NULL , 
     paid_patient_deductible			NUMERIC			NULL , 
     paid_by_primary						NUMERIC			NULL , 
     paid_ingredient_cost				NUMERIC			NULL , 
     paid_dispensing_fee					NUMERIC			NULL , 
     payer_plan_period_id			INTEGER			NULL ,
     amount_allowed		NUMERIC			NULL , 
     revenue_code_concept_id		INTEGER			NULL , 
     reveue_code_source_value    VARCHAR(50)   NULL ,
	 drg_concept_id			INTEGER		NULL,
     drg_source_value		VARCHAR(3)		NULL
    ) 
;





CREATE TABLE drug_era
    ( 
     drug_era_id					INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     drug_concept_id				INTEGER			NOT NULL , 
     drug_era_start_date			DATE			NOT NULL , 
     drug_era_end_date				DATE			NOT NULL , 
     drug_exposure_count			INTEGER			NULL ,
	 gap_days						INTEGER			NULL
    ) 
;


CREATE TABLE condition_era
    ( 
     condition_era_id				INTEGER			NOT NULL , 
     person_id						INTEGER			NOT NULL , 
     condition_concept_id			INTEGER			NOT NULL , 
     condition_era_start_date		DATE			NOT NULL , 
     condition_era_end_date			DATE			NOT NULL , 
     condition_occurrence_count		INTEGER			NULL
    ) 
;


COPY CARE_SITE FROM '/postgresql_data/care_site.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY CDM_SOURCE FROM '/postgresql_data/cdm_source.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY CONDITION_ERA FROM '/postgresql_data/condition_era.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY CONDITION_OCCURRENCE FROM '/postgresql_data/condition_occurrence.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY COST FROM '/postgresql_data/cost.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY DEATH FROM '/postgresql_data/death.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY DEVICE_EXPOSURE FROM '/postgresql_data/device_exposure.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY DRUG_ERA FROM '/postgresql_data/drug_era.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY DRUG_EXPOSURE FROM '/postgresql_data/drug_exposure.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY LOCATION FROM '/postgresql_data/location.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY MEASUREMENT FROM '/postgresql_data/measurement.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY OBSERVATION FROM '/postgresql_data/observation.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY OBSERVATION_PERIOD FROM '/postgresql_data/observation_period.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY PAYER_PLAN_PERIOD FROM '/postgresql_data/payer_plan_period.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY PERSON FROM '/postgresql_data/person.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY PROCEDURE_OCCURRENCE FROM '/postgresql_data/procedure_occurrence.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY PROVIDER FROM '/postgresql_data/provider.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
COPY VISIT_OCCURRENCE FROM '/postgresql_data/visit_occurrence.csv' DELIMITER E'\t' CSV ENCODING 'UTF8';
