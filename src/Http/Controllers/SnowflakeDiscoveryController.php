<?php

namespace DreamFactory\Core\Snowflake\Http\Controllers;

use DreamFactory\Core\Snowflake\Database\Connectors\SnowflakeOdbcConnector;
use DreamFactory\Core\Snowflake\Database\SnowflakeOdbcConnection;
use DreamFactory\Core\Snowflake\Utility\SnowflakeNativeAppDetector;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;

/**
 * Discovery endpoints for the Snowflake Native App service-creation picker.
 *
 * Uses the container's own SPCS identity (/snowflake/session/token) to enumerate
 * what the app role has been granted. Does NOT require an existing DF service —
 * these run before a service is created, so admins can pick from the list
 * instead of typing database / schema / warehouse / role values.
 */
class SnowflakeDiscoveryController extends Controller
{
    /**
     * Per-request cached connection. Reuse across endpoints on the same request.
     *
     * @var SnowflakeOdbcConnection|null
     */
    protected $connection = null;

    /**
     * GET /api/v2/_snowflake/discover/databases
     *
     * Returns databases visible to the native app's role.
     */
    public function databases(Request $request)
    {
        return $this->run(function (SnowflakeOdbcConnection $conn) {
            $rows = $this->showRows($conn, 'SHOW DATABASES');
            return array_map(function ($row) {
                return [
                    'name'       => $this->pick($row, 'name'),
                    'comment'    => $this->pick($row, 'comment'),
                    'owner'      => $this->pick($row, 'owner'),
                    'created_on' => $this->pick($row, 'created_on'),
                    'kind'       => $this->pick($row, 'kind'),
                ];
            }, $rows);
        });
    }

    /**
     * GET /api/v2/_snowflake/discover/schemas?database=MY_DB
     *
     * Returns schemas inside the given database.
     */
    public function schemas(Request $request)
    {
        $database = $request->query('database');
        if (empty($database)) {
            return response()->json(['error' => 'database query parameter is required'], 400);
        }

        if (!$this->isValidIdentifier($database)) {
            return response()->json(['error' => 'Invalid database identifier'], 400);
        }

        return $this->run(function (SnowflakeOdbcConnection $conn) use ($database) {
            $quoted = $this->quoteIdentifier($database);
            $rows = $this->showRows($conn, "SHOW SCHEMAS IN DATABASE {$quoted}");
            return array_map(function ($row) {
                return [
                    'name'          => $this->pick($row, 'name'),
                    'database_name' => $this->pick($row, 'database_name'),
                    'comment'       => $this->pick($row, 'comment'),
                    'owner'         => $this->pick($row, 'owner'),
                ];
            }, $rows);
        });
    }

    /**
     * GET /api/v2/_snowflake/discover/warehouses
     *
     * Returns the native app's assigned warehouse. In the native-app context
     * this is fixed by the consumer admin at install time via the warehouse
     * reference callback (surfaced as SNOWFLAKE_WAREHOUSE in the container env).
     *
     * We deliberately do NOT run `SHOW WAREHOUSES` — the Snowflake ODBC driver
     * has a pre-allocation bug that can OOM the PHP process on account-wide
     * SHOW results. The app realistically only ever uses one warehouse anyway.
     */
    public function warehouses(Request $request)
    {
        try {
            if (!SnowflakeNativeAppDetector::isNativeApp()) {
                return response()->json([
                    'error' => 'Discovery endpoints require the Snowflake Native App environment.',
                ], 503);
            }

            $config = SnowflakeNativeAppDetector::getNativeAppConfig();
            $warehouse = $config['warehouse'] ?? null;

            $resource = [];
            if (!empty($warehouse)) {
                $resource[] = [
                    'name'  => $warehouse,
                    'state' => null,
                    'size'  => null,
                    'type'  => null,
                ];
            }

            return response()->json(['resource' => $resource]);
        } catch (\Throwable $e) {
            \Log::error('Snowflake warehouse discovery failed', [
                'message' => $e->getMessage(),
            ]);
            return response()->json([
                'error'   => 'Warehouse discovery failed',
                'message' => $e->getMessage(),
            ], 500);
        }
    }

    /**
     * GET /api/v2/_snowflake/discover/roles
     *
     * Returns roles visible to the native app. In practice this will be the
     * app's own role and any roles granted to it.
     */
    public function roles(Request $request)
    {
        return $this->run(function (SnowflakeOdbcConnection $conn) {
            $rows = $this->showRows($conn, 'SHOW ROLES');
            return array_map(function ($row) {
                return [
                    'name'    => $this->pick($row, 'name'),
                    'comment' => $this->pick($row, 'comment'),
                    'owner'   => $this->pick($row, 'owner'),
                ];
            }, $rows);
        });
    }

    /**
     * Run a SHOW statement and return rows with keys lowercased for case-insensitive access.
     */
    protected function showRows(SnowflakeOdbcConnection $conn, string $sql): array
    {
        $rows = $conn->select($sql);
        return array_map(function ($row) {
            $assoc = (array) $row;
            $lower = [];
            foreach ($assoc as $k => $v) {
                $lower[strtolower($k)] = $v;
            }
            return $lower;
        }, $rows);
    }

    /**
     * Case-insensitive column lookup on a normalized row.
     */
    protected function pick(array $row, string $key)
    {
        return $row[strtolower($key)] ?? null;
    }

    /**
     * Shared runner: open the SPCS connection, execute, shape response, handle errors.
     */
    protected function run(callable $query)
    {
        try {
            if (!SnowflakeNativeAppDetector::isNativeApp()) {
                return response()->json([
                    'error' => 'Discovery endpoints require the Snowflake Native App environment.',
                ], 503);
            }

            $conn = $this->getConnection();
            $data = $query($conn);

            return response()->json(['resource' => $data]);
        } catch (\Throwable $e) {
            \Log::error('Snowflake discovery failed', [
                'message' => $e->getMessage(),
                'trace'   => $e->getTraceAsString(),
            ]);
            return response()->json([
                'error'   => 'Discovery query failed',
                'message' => $e->getMessage(),
            ], 500);
        }
    }

    /**
     * Build a SPCS-native config and open an ODBC connection. Cached per request.
     */
    protected function getConnection(): SnowflakeOdbcConnection
    {
        if ($this->connection !== null) {
            return $this->connection;
        }

        $config = SnowflakeNativeAppDetector::getNativeAppConfig();

        // Read the SPCS session token fresh — Snowflake keeps it rotated.
        $tokenPath = '/snowflake/session/token';
        if (!file_exists($tokenPath) || !is_readable($tokenPath)) {
            throw new \RuntimeException('SPCS session token not available at ' . $tokenPath);
        }
        $token = trim(file_get_contents($tokenPath));
        if (empty($token)) {
            throw new \RuntimeException('SPCS session token file is empty');
        }

        $config['oauth_access_token'] = $token;
        $config['authenticator']      = 'oauth';
        $config['use_odbc']           = true;

        $connector    = new SnowflakeOdbcConnector();
        $odbcResource = $connector->connect($config);

        $this->connection = new SnowflakeOdbcConnection(
            $odbcResource,
            $config['database'] ?? '',
            '',
            $config
        );

        return $this->connection;
    }

    /**
     * Validate that a Snowflake identifier is safe to interpolate.
     * Accepts: unquoted identifiers matching Snowflake's rules (letters, digits, _, $, starts with letter/_),
     *          or already-quoted identifiers ("...").
     */
    protected function isValidIdentifier(string $identifier): bool
    {
        if ($identifier === '' || strlen($identifier) > 255) {
            return false;
        }
        // Already quoted — allow if no stray quotes besides the wrapping pair
        if (preg_match('/^"([^"]|"")+"$/', $identifier)) {
            return true;
        }
        // Unquoted — standard Snowflake rules
        return (bool) preg_match('/^[A-Za-z_][A-Za-z0-9_$]*$/', $identifier);
    }

    /**
     * Quote a Snowflake identifier for safe interpolation into SHOW statements.
     */
    protected function quoteIdentifier(string $identifier): string
    {
        // Already quoted — pass through
        if (preg_match('/^".*"$/', $identifier)) {
            return $identifier;
        }
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
