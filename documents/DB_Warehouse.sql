-- Create DB "Warehouse"

use master;

Create Database DataWarehouse1;

-- Switch to the new DB
Use DataWarehouse1;

go
create schema bronze;
go
create schema silver;
go
create schema gold;