
INSERT INTO device_exposure
(
	device_exposure_id,
	person_id,
	device_concept_id,
	device_exposure_start_date,
	device_exposure_start_datetime,
	device_exposure_end_date,
	device_exposure_end_datetime,
	device_type_concept_id,
	unique_device_id,
	quantity,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	device_source_value,
	device_source_concept_id
)
SELECT
	stem_table.id	AS	device_exposure_id,

	stem_table.person_id	AS	person_id,

	stem_table.concept_id	AS	device_concept_id,

	stem_table.start_date	AS	device_exposure_start_date,

	stem_table.start_datetime	AS	device_exposure_start_datetime,

	stem_table.end_date	AS	device_exposure_end_date,

	stem_table.end_datetime	AS	device_exposure_end_datetime,

	stem_table.type_concept_id	AS	device_type_concept_id,

	stem_table.unique_device_id	AS	unique_device_id,

	stem_table.quantity	AS	quantity,

	stem_table.provider_id	AS	provider_id,

	stem_table.visit_occurrence_id	AS	visit_occurrence_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	visit_detail_id,

	stem_table.source_value	AS	device_source_value,

	stem_table.source_concept_id	AS	device_source_concept_id

FROM stem_table
;