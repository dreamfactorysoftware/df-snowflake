# df-snowflake
DreamFactory Snowflake Database Service

This code is governed by a commercial license. To use it, you must follow refer to the LICENSE file.

## Configure Snowflake

To connect your Snowflake database to Dreamfactory, you will need to specify:

1) Hostname
An optional hostname that can be used as an alternative to the snowflake default hostname.
2) Account
Account is the [hostname + region](https://docs.snowflake.com/en/user-guide/intro-regions.html#specifying-region-information-in-your-account-hostname) information.  
You can just copy it from your snowflake database URL:  

![accountexample](https://img.in6k.com/screens/3caa9f72_2021.04.02.png)


3) Username
Username that you use to login to your snowflake account or any other user with access to the database.
4) Password
Password for your snowflake account (only if not using key pair authentication).
5) Database
Name of the database you want to connect to.
6) Warehouse
Name of the warehouse your database uses.
7) Schema (optional)
Schema of the database, PUBLIC by default.

## Key Pair Authentication

DreamFactory supports Snowflake's key pair authentication method, which can be more secure than password-based authentication. To use key pair authentication:

1. Generate a key pair for Snowflake authentication:
   ```
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```

2. In Snowflake, assign the public key to your user:
   ```sql
   ALTER USER <username> SET RSA_PUBLIC_KEY='<public_key_data>';
   ```
   Replace `<public_key_data>` with the contents of your public key (rsa_key.pub), ensuring all line breaks are removed.

3. In DreamFactory:
   - Specify your Snowflake account, username, and other connection details
   - Leave the password field blank
   - Upload your private key file (rsa_key.p8) in the "Private Key File" field
   - If your private key is encrypted, enter the passphrase in the "Private Key Passphrase" field

For more information about Snowflake key pair authentication, see the [official Snowflake documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth).
