
INSERT INTO drug_exposure
(
	drug_exposure_id,
	person_id,
	drug_concept_id,
	drug_exposure_start_date,
	drug_exposure_start_datetime,
	drug_exposure_end_date,
	drug_exposure_end_datetime,
	verbatim_end_date,
	drug_type_concept_id,
	stop_reason,
	refills,
	quantity,
	days_supply,
	sig,
	route_concept_id,
	lot_number,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	drug_source_value,
	drug_source_concept_id,
	route_source_value,
	dose_unit_source_value
)
SELECT
	stem_table.id	AS	drug_exposure_id,

	stem_table.person_id	AS	person_id,

	stem_table.concept_id	AS	drug_concept_id,

	stem_table.start_date	AS	drug_exposure_start_date,

	stem_table.start_datetime	AS	drug_exposure_start_datetime,

	stem_table.end_date	AS	drug_exposure_end_date,

	stem_table.end_datetime	AS	drug_exposure_end_datetime,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	verbatim_end_date,

	stem_table.type_concept_id	AS	drug_type_concept_id,

	stem_table.stop_reason	AS	stop_reason,

	stem_table.refills	AS	refills,

	stem_table.quantity	AS	quantity,

	stem_table.days_supply	AS	days_supply,

	stem_table.sig	AS	sig,

	stem_table.route_concept_id	AS	route_concept_id,

	stem_table.lot_number	AS	lot_number,

	stem_table.provider_id	AS	provider_id,

	stem_table.visit_occurrence_id	AS	visit_occurrence_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	visit_detail_id,

	stem_table.source_value	AS	drug_source_value,

	stem_table.source_concept_id	AS	drug_source_concept_id,

	stem_table.route_source_value	AS	route_source_value,

	stem_table.dose_unit_source_value	AS	dose_unit_source_value

FROM stem_table
;