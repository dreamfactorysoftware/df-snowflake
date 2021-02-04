# df-snowflake
DreamFactory Snowflake Database Service

This code is governed by a commercial license. To use it, you must follow refer to the LICENSE file.

## Configure Snowflake

To connect your Snowflake database to Dreamfactory, you will need to specify:  
1) Account  
Account is the [hostname + region](https://docs.snowflake.com/en/user-guide/intro-regions.html#specifying-region-information-in-your-account-hostname) information.  
You can just copy it from your snowflake database URL:  

![accountexample](https://img.in6k.com/screens/3caa9f72_2021.04.02.png)


2) Username  
Username that you use to login to your snowflake account or any other user with access to the database.
3) Password  
4) Database  
Name of the database you want to connect to.
5) Warehouse  
Name of the warehouse your database uses.
6) Schema (optional)  
Schema of the database, PUBLIC by default.
