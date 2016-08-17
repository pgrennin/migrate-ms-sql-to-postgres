# migrate-ms-sql-to-postgres
A Ruby script to sync data from MS SQL Server tables to Postgresql table.

Uses Sequel gem to connect to Sql Server and Postgres database.  Iterates through MS SQL table data and run sql insert statements against postgres database.

Useful for database conversion from MS SQL Server to Postgresql or just syncing data between databases.
