
# Rabbit-In-a-Hat

![](https://www.ohdsi.org/web/wiki/lib/exe/fetch.php?media=documentation:software:rabbitinahatlogo.png )


## Introduction


### Scope and purpose

Rabbit-In-a-Hat comes with WhiteRabbit and is designed to read and display a WhiteRabbit scan document. WhiteRabbit generates information about the source data while Rabbit-In-a-Hat uses that information and through a graphical user interface to allow a user to connect source data to tables and columns within the CDM. Rabbit-In-a-Hat generates documentation for the ETL process it does not generate code to create an ETL.


### Process Overview

The typical sequence for using this software to generate documentation of an ETL:
  - Scanned results from WhiteRabbit completed
  - Open scanned results; interface displays source tables and CDM tables
  - Connect source tables to CDM tables where the source table provides information for that corresponding CDM table
  - For each source table to CDM table connection, further define the connection with source column to CDM column detail
  - Save Rabbit-In-a-Hat work and export to a MS Word document.


## Installation and support

Rabbit-In-a-Hat comes with WhiteRabbit, refer to WhiteRabbit's installation section.


## Getting Started


### Creating a New Document

To create a new document, navigate to _File --> Open Scan Report_. Use the “Open” window to browse for the scan document created by WhiteRabbit. When a scan document is open, the tables scanned will appear in orange boxes on the “Source” side of the Tables.

Save the Rabbit-In-a-Hat document by going _File --> Save as_.


### Open an Existing Document

To open an existing Rabbit-In-a-Hat document use _File --> Open ETL specs_.



### Selecting Desired CDM Version

Rabbit-In-a-Hat allows you to select which CDM version (v4, v5 or v6) you'd like to built your ETL specification against.

See the graphic below for how to select your desired CDM:
![Switching between CDMv4 and CDMv5](http://i.imgur.com/LOqhp7H.gif)


The CDM version can be changed at any time, but beware that some of your existing mappings may be lost in the process.  By default, Rabbit-In-a-Hat will attempt to pereserve as many mappings between the source data and the newly selected CDM as possible.  When a new CDM is selected, Rabbit-In-a-Hat will drop any mappings if the mapping's CDM table or CDM column name no longer exist

For instance, switching from CDMv4 to CDMv5, a mapping from source to CDM person.person_source_value will be kept because the person table has person_source_value in both CDMv4 and CDMv5.  However, person.assocaited_provider_id exists only in CDMv4 and has been renamed to [person.provider_id in CDMv5](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:person) and so that mapping will not be kept when switching between these two CDMs.


### Loading in a Custom CDM

There are times when users might need to load in a customized version of the CDM, for instance if they are sandboxing new features.  To load in a custom CDM schema, first you must create a CSV file that uses the same format as [the existing CDMv5 schema file](https://github.com/OHDSI/WhiteRabbit/blob/master/src/org/ohdsi/rabbitInAHat/dataModel/CDMV5.csv).

Once you have created the CSV file, load it into RiaH as shown below:
![Loading a custom CDM schema](http://i.imgur.com/Tn9NKL3.gif)

Please note that the name of the file you load in becomes the label that appears above the target tables, so "My Super File.csv" will create the label "My Super File" above the target tables, so name your CSV accordingly.


### Stem table
TODO
Can be added and/or removed.
Explain how to use.


### Concept id hints
TODO


## Connecting Source Tables to CDM Tables

It is assumed that the owners of the source data should be able to provide detail of what the data table contains, Rabbit-In-a-Hat will describe the columns within the table but will not provide the context a data owner should provide. For the CDM tables, if more information is needed navigate to the OMOP CDM web page (http://omop.org/CDM) and review the current OMOP specification.

To connect a source table to a CDM table, simply hover over the source table until an arrow head appears.

![](https://www.ohdsi.org/web/wiki/lib/exe/fetch.php?media=documentation:software:rabbitinahat-drugclaims.png )

Use your mouse to grab the arrow head and drag it to the corresponding CDM table. In the example below, the _drug_claims_ data will provide information for the _drug_exposure_ table.

![](https://www.ohdsi.org/web/wiki/lib/exe/fetch.php?media=documentation:software:rabbitinahat-drugclaimsdrugexposure.png )

If you click on the arrow once it will highlight and a _Details_ window will appear in the right pane. Use this to describe _Logic or Comments_ that someone developing the ETL code should know about this source table to CDM table mapping.

![](https://www.ohdsi.org/web/wiki/lib/exe/fetch.php?media=documentation:software:rabbitinahat-arrow.png )

Continue this process until all tables that are needed to build a CDM are mapped to their corresponding CDM tables. One source table can map to multiple CDM tables and one CDM table can receive multiple mappings. There may be tables in the source data that should not be map into the CDM and there may be tables in the CDM that cannot be populated from the source data.


## Connecting Source Fields to CDM Fields

By double clicking on an arrow connecting a source and CDM table, it will open a _Fields_ pane below the arrow selected. The _Fields_ pane will have all the source table and CDM fields and is meant to make the specific column mappings between tables. Hovering over a source table will generate an arrow head that can then be selected and dragged to its corresponding CDM field. For example, in the _drug_claims_ to _drug_exposure_ table mapping example, the source data owners know that _patient_id_ is the patient identifier and corresponds to the _CDM.person_id_. Also, just as before, the arrow can be selected and _Logic_ and _Comments_ can be added.

![](https://www.ohdsi.org/web/wiki/lib/exe/fetch.php?media=documentation:software:rabbitinahat-fields.png )

If you select the source table orange box, Rabbit-In-a-Hat will expose values the source data has for that table. This is meant to help in the process in understanding the source data and what logic may be required to handle the data in the ETL. In the example below _ndcnum_ is selected and raw NDC codes are displayed starting with most frequent (note that in the WhiteRabbit scan a “Min cell count” could have been selected and values below that frequency will not show).

![](https://www.ohdsi.org/web/wiki/lib/exe/fetch.php?media=documentation:software:rabbitinahat-fieldex.png )

Continue this process until all source columns necessary in all mapped tables have been mapped to the corresponding CDM column. Not all columns must be mapped into a CDM column and not all CDM columns require a mapping. One source column may supply information to multiple CDM columns and one CDM column can receive information from multiple columns.


## Generating an ETL Document

To generate an ETL MS Word document use _File --> Generate ETL_ document and select a location to save. It may take a moment before the document is finished creating. This document will contain all notes from Rabbit-In-a-Hat.

Once the information is in the document, if an update is needed you must either update the information in Rabbit-In-a-Hat and regenerate the document or update the document. If you make changes in the document, Rabbit-In-a-Hat will not read those changes and update the information in the tool. However it is common to generate the document with the core mapping information and fill in more detail within the document.

Once the document is completed, this should be shared with the individuals who plan to implement the code to execute the ETL.


## Generating a testing framework

To make sure the ETL process is working as specified, it is highly recommended to create [unit tests](https://en.wikipedia.org/wiki/Unit_testing) that evaluate the behavior of the ETL process.
To efficiently create a set of unit tests Rabbit-in-a-Hat can [generate a testing framework](documentation:software:whiterabbit:test_framework).

**TODO**: Link to generate testing framework page


## Generating a sql skeleton
**TODO**
