# SSIS Project for Flush & Fill Data Warehouse
This SSIS project contains two packages that perform same ETL process but in two different ways. Source is AdventureWorks_Basics database and destination is DWAdventureWorks_Basics data warehouse. ETL process is based on SQL script created in Assignment 1 of this course. Purpose of this assignment is to explore two different ways of developing SSIS Packages for the same ETL Process and to compare them.  
In both packages, ETL process is done in 4 stages:
1.	Prepare the data warehouse for the ETL process: dropping all constrains between tables and truncate all the tables
2.	Fill Dimension tables: populating the dimension tables with data one by one. They are independent tables so there is no specific order of data population
3.	Fill Fact table: populating the fact table with data
4.	Ending the ETL process: creating all constrains between tables

## Package that uses SQL code - AdventureWorksETLWithSQLCode.dtsx
This package uses stored procedures created for ETL SQL script (Figure 1). Those procedures are called with Execute SQL Tasks. This package uses just destination ADO.NET connection. All connectons fror extracting data are in the SQL stored procedures.
Each stored procedure has returning value to provide the stored procedure’s execution status to the SSIS Execute SQL Task (0 not executed, 1 executed with success, -1 error in execution process). Return code is read by Script Task and logged in log file ExecProc.log. In order to reuse the same variable for each Execute SQL Task as return code, each SQL Task & Script Task are in own Sequence Container. 
Precedence Constraints between Filling dimension table’s containers are set up upon completion status because all those tasks are independent from each other. 

## Package that does not contain SQL code - AdventureWorksETLWithSSISTransformations.dtsx
This package pulls the data directly from the source database and uses SSIS Transformations and destinations to fill the data warehouse. In this package all transformations, joins, loops are done with Data Flow Tasks. SQL code for dropping and creating constraints and code for truncate tables is directly written as SQL statement in Execute SQL Task.
Each Data Flow task encapsulates the data flow engine that moves data between source and destination, and lets the user transform, clean, and modify data as it is moved.
