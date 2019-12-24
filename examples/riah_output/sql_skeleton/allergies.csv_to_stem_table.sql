
INSERT INTO stem_table
(
	domain_id,
	person_id,
	visit_occurrence_id,
	provider_id,
	id,
	concept_id,
	source_value,
	source_concept_id,
	type_concept_id,
	start_date,
	start_datetime,
	end_date,
	end_datetime,
	verbatim_end_date,
	days_supply,
	dose_unit_source_value,
	lot_number,
	modifier_concept_id,
	operator_concept_id,
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id, -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
	modifier_concept_id -- [!#WARNING!#] THIS TARGET FIELD WAS ALREADY USED
)
SELECT
 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	domain_id,

	allergies.csv.patient	AS	person_id,

	allergies.csv.encounter	AS	visit_occurrence_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	provider_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	id,

	allergies.csv.code	AS	concept_id,

	allergies.csv.code	AS	source_value,

	allergies.csv.code	AS	source_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	type_concept_id,

	allergies.csv.start	AS	start_date,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	start_datetime,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	end_date,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	end_datetime,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	verbatim_end_date,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	days_supply,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	dose_unit_source_value,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	lot_number,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	operator_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	modifier_concept_id

FROM allergies.csv
;