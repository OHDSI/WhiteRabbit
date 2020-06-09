---
title: Best Practices
output:
  html_document:
    toc: false
---

The following lists best practices in using WhiteRabbit and Rabbit-In-a-Hat to manage your ETL documentation process:

  * **Overall Process:**
    * When going through the ETL exercise, it is critical to get all key stakeholders in the room such as the data owners and individuals who plan to perform/manage research using the CDM. We have found different stakeholders have different perspectives on source data and the conversation that occurs improves the ETL process.
  * **WhiteRabbit:**
    * If it is known some tables will not be needed from the source data, do not include them in the scan. If there is a question to if the table is necessary it is better to include it.
  * **Rabbit-In-a-Hat:**
    * Start with mapping tables to tables and then dive into how one table’s fields map into CDM fields. In other words, stay at the table level until all tables a mapped, and then start to map fields.
    * If your source data does not contain certain information you do not need to impute or generate information to fulfil a CDM requirement. For example, if your source data does not contain an end date for when a medication was stopped you do not need to population the DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATE column.
    * Derived CDM tables, like DRUG_ERA, typically will not receive a mapping from the source data because they are generated off the CDM table (in this case DRUG_ERA is generated off DRUG_EXPOSURE).

References:

* ETL implementation best practices: <https://www.ohdsi.org/web/wiki/doku.php?id=documentation:etl_best_practices>
* Example ETLs: <https://www.ohdsi.org/web/wiki/doku.php?id=documentation:example_etls>
* Ask your ETL questions on the implementers forum: <https://forums.ohdsi.org/c/implementers>
