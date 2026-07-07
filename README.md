# df-snowflake
DreamFactory Snowflake Database Service

This code is governed by a commercial license. To use it, refer to the LICENSE file.

## Overview

DreamFactory is a secure, self-hosted enterprise data access platform that provides governed API access to any data source, connecting enterprise applications and on-prem LLMs with role-based access and identity passthrough.

This package connects DreamFactory to Snowflake via the official Snowflake PHP PDO driver (`pdo_snowflake`) — a compiled C extension that speaks Snowflake's native protocol. One connection per service instance per HTTP request (no pooling).

## Requirements

- **Gold license** — the Snowflake service type is only available on Gold tier.
- The `pdo_snowflake` PHP extension must be installed and loaded. Confirm with `php -m | grep snowflake`. It ships in the official DreamFactory Docker images.

## Configure Snowflake

Create the service in the admin console: **Services > Create > Database > Snowflake**, then fill the config form:

| Field | Required | Notes |
|-------|----------|-------|
| Account | Yes | Snowflake [account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier) (e.g. `xy12345.us-east-1` or the org-account form `MYORG-MYACCT`). Supports lookup keys. |
| Hostname | No | Custom endpoint for PrivateLink or vanity URLs. Leave blank to derive the host from Account. |
| Username | Yes | Snowflake user with access to the database. Supports lookup keys. |
| Password | Conditional | Required for password auth. Leave blank for key-pair auth. |
| Private Key File | Conditional | Upload/select a `.pem`/`.p8` file for key-pair auth. |
| Private Key Passphrase | No | Only if the key file is encrypted. |
| Role | No | Snowflake role to assume on connection. |
| Database | Yes | Target database. Supports lookup keys. |
| Warehouse | Yes | Compute warehouse. |
| Schema | No | Defaults to `PUBLIC` (Snowflake's default schema) if left blank. |

`username`, `password`, `key`, and `passcode` are encrypted at rest; `password` is additionally masked (never returned in API responses).

## Authentication

### Key-pair (recommended)

> **Heads up:** Snowflake is retiring single-factor password auth for `TYPE=SERVICE` users in **October 2026**. Use key-pair (or OAuth/PAT) for service accounts.

1. Generate a key pair:
   ```
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```
   (Omit `-nocrypt` to encrypt the private key; then set the passphrase in the config.)

2. Register the public key on your Snowflake user (strip the PEM header/footer and all line breaks):
   ```sql
   ALTER USER <username> SET RSA_PUBLIC_KEY='<public_key_data>';
   ```

3. In DreamFactory: fill Account/Username/Database/Warehouse/Schema, **leave Password blank**, upload the private key in **Private Key File**, and set the passphrase only if the key is encrypted.

DreamFactory generates the JWT internally via `pdo_snowflake` (`authenticator=SNOWFLAKE_JWT`). Private keys are stored at `storage/app/keys/snowflake/` with `0600` permissions.

### Password

Fill the Password field and leave the key fields blank. Standard PDO auth.

## Session Parameters (Additional SQL Statements)

The **Additional SQL Statements** config field runs an array of SQL statements on every connection, before any request. Use it to set session parameters:

```
ALTER SESSION SET TIMEZONE = 'UTC'
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300
ALTER SESSION SET ROWS_PER_RESULTSET = 10000
```

Common uses: pinning `TIMEZONE` for consistent timestamp handling, capping runaway queries with a statement timeout, and bounding result size.

## Query Tagging

Every connection is automatically tagged so you can attribute Snowflake cost and usage per DreamFactory service. The tag is a JSON object set via `ALTER SESSION SET QUERY_TAG`:

```json
{"app":"dreamfactory","service":"<service name>","database":"...","schema":"...","warehouse":"..."}
```

Attribute usage by querying account usage:

```sql
SELECT query_tag, COUNT(*), SUM(credits_used_cloud_services)
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag ILIKE '%dreamfactory%'
GROUP BY 1;
```

Tagging is best-effort — if it fails, the connection is unaffected.

## Runtime Overrides (multi-tenant)

Any connection field can be overridden per-request, letting one service definition serve multiple databases/schemas/warehouses:

- **Headers:** `hostname`, `account`, `database`, `schema`, `warehouse`, `username`, `password`, `key`, `passcode`, `role`.
- **URL query params:** same fields **except `key` and `passcode`** — key material is deliberately header-only so it never lands in URLs, logs, or referrers.

Header values take precedence over URL params.

## Supported Features

- Create, read, and delete on tables (GET, POST, DELETE). PUT/PATCH updates are currently hidden from the API docs pending verification.
- Schema introspection
- Stored procedures and functions (cross-database via the `X-Database-Name` header)
- Snowflake Cortex AI functions (COMPLETE, CLASSIFY_TEXT, SENTIMENT, etc.)
- VARIANT/OBJECT/ARRAY columns auto-decoded from JSON
- `ILIKE` filter operator
- Parameterized queries throughout

For more on Snowflake key-pair auth, see the [official Snowflake documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth).
