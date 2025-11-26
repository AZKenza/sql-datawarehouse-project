/*
==========================================================================================================================
Create DataBase and Schemas
==========================================================================================================================

Warning:
Running this setup will delete your entire existing DataWarehouse database and all its contents before creating a fresh one. Make sure to back up any critical data before proceeding.

What This Script Does
Drops any existing DataWarehouse database (including all tables and data).

Creates a new, empty DataWarehouse database.

Adds three schemas: bronze, silver, and gold.

Provides instructions for connecting securely.
*/

PostgreSQL Data Warehouse Initialization
This repository automates the setup of a PostgreSQL data warehouse with a medallion architecture, including Bronze, Silver, and Gold schemas.

Steps Performed
1. Drop Existing DataWarehouse Database (If Exists)
Ensures a clean state by dropping the DataWarehouse database if it already exists:

  DROP DATABASE IF EXISTS "DataWarehouse";

2. Create New DataWarehouse Database

  CREATE DATABASE "DataWarehouse";

3. Create Bronze, Silver, and Gold Schemas
After connecting to the new database, creates the three schemas:

  CREATE SCHEMA IF NOT EXISTS bronze;
  CREATE SCHEMA IF NOT EXISTS silver;
  CREATE SCHEMA IF NOT EXISTS gold;

4. Connect Using Connection String
The default Postgres connection string for this setup:
 
  postgresql://<USER>:<PASSWORD>@<HOST>:<PORT>/DataWarehouse
