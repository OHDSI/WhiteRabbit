Introduction
========
This repository forked from https://github.com/OHDSI/WhiteRabbit. 

**WhiteRabbit** is a small application that can be used to analyse the structure and contents of a database as preparation for designing an ETL. 

This service wraps WhiteRabbit functional in Web-service, that used by **Perseus** https://github.com/SoftwareCountry/Perseus. 

Features
========
- Can scan databases in SQL Server, Oracle, PostgreSQL, MySQL, MS Access, Amazon RedShift, Google BigQuery, SAS files and CSV files
- The scan report contains information on tables, fields, and frequency distributions of values
- Cutoff on the minimum frequency of values to protect patient privacy

Technology
============

- Java 15

Getting Started
===============

    docker build -t white-rabbit-service .
    docker run --name white-rabbit-service -d --network host white-rabbit-service

License
=======
WhiteRabbit is licensed under Apache License 2.0
