/*
=====================
Create Schemas
=====================
Script Purpose:
This script creates the logical schema layers used in the medallion architecture
(bronze, silver, and gold). In MySQL, these layers are implemented as separate
databases and are created once during environment setup.
*/
-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;



