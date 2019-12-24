
INSERT INTO specimen
(
	specimen_id,
	person_id,
	specimen_concept_id,
	specimen_type_concept_id,
	specimen_date,
	specimen_datetime,
	quantity,
	unit_concept_id,
	anatomic_site_concept_id,
	disease_status_concept_id,
	specimen_source_id,
	specimen_source_value,
	unit_source_value,
	anatomic_site_source_value,
	disease_status_source_value
)
SELECT
	stem_table.id	AS	specimen_id,

	stem_table.person_id	AS	person_id,

	stem_table.concept_id	AS	specimen_concept_id,

	stem_table.type_concept_id	AS	specimen_type_concept_id,

	stem_table.start_date	AS	specimen_date,

	stem_table.start_datetime	AS	specimen_datetime,

	stem_table.quantity	AS	quantity,

	stem_table.unit_concept_id	AS	unit_concept_id,

	stem_table.anatomic_site_concept_id	AS	anatomic_site_concept_id,

	stem_table.disease_status_concept_id	AS	disease_status_concept_id,

	stem_table.specimen_source_id	AS	specimen_source_id,

	stem_table.source_value	AS	specimen_source_value,

	stem_table.unit_source_value	AS	unit_source_value,

	stem_table.anatomic_site_source_value	AS	anatomic_site_source_value,

	stem_table.disease_status_source_value	AS	disease_status_source_value

FROM stem_table
;