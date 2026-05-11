<?php

namespace DreamFactory\Core\Snowflake\Tests\Security;

use PHPUnit\Framework\TestCase;

/**
 * Security: SnowflakeSchema::loadParameters() must NOT use eval() and must
 * parameterize the information-schema lookup.
 *
 * Phase 2 audit (df-snowflake P0/P1) found:
 *
 *   1. SQL string interpolated `$holder->resourceName` and `$holder->schemaName`
 *      directly into the WHERE clause:
 *         WHERE {$type}_NAME = '{$holder->resourceName}' AND {$type}_SCHEMA = '{$holder->schemaName}'
 *
 *   2. The ARGUMENT_SIGNATURE returned from Snowflake was passed to eval():
 *         $argumentSignature = str_replace(['(', ')'], '"', ...);
 *         eval('$arguments = explode( ", ", ' . $argumentSignature . ');');
 *
 *      The str_replace converts `(...)` to `"..."` but does NOT escape
 *      embedded PHP metacharacters. A routine author who can craft an
 *      ARGUMENT_SIGNATURE that contains PHP syntax (closing the string,
 *      injecting code) gets remote code execution in the DreamFactory
 *      process.
 *
 * After the fix:
 *   1. WHERE clause uses :name and :schema bindings.
 *   2. ARGUMENT_SIGNATURE is split via trim('()') + explode(',') — no eval.
 */
class SnowflakeLoadParametersTest extends TestCase
{
    private string $sourcePath;
    private string $contents;

    protected function setUp(): void
    {
        $this->sourcePath = __DIR__ . '/../../src/Database/Schema/SnowflakeSchema.php';
        $this->assertFileExists($this->sourcePath);
        $this->contents = file_get_contents($this->sourcePath);
    }

    private function loadParametersBody(): string
    {
        $start = strpos($this->contents, 'function loadParameters');
        $this->assertNotFalse($start, 'loadParameters() must exist');
        $next = strpos($this->contents, "\n    /**", $start + 10);
        if ($next === false) {
            $next = strpos($this->contents, "\n    public function ", $start + 10);
        }
        if ($next === false) {
            $next = strpos($this->contents, "\n    protected function ", $start + 10);
        }
        return substr($this->contents, $start, $next === false ? null : ($next - $start));
    }

    public function testLoadParametersDoesNotUseEval(): void
    {
        $body = $this->loadParametersBody();
        // Strip line/block comments before checking — the `eval` keyword may
        // appear in a comment explaining the previous behaviour.
        $stripped = preg_replace('!//[^\n]*!', '', $body);
        $stripped = preg_replace('!/\*.*?\*/!s', '', $stripped);

        $this->assertDoesNotMatchRegularExpression(
            '/\beval\s*\(/',
            $stripped,
            'loadParameters() must not use eval(). Use trim/explode to parse '
            . 'ARGUMENT_SIGNATURE without invoking the PHP parser on data.'
        );
    }

    public function testLoadParametersParameterizesRoutineAndSchema(): void
    {
        $body = $this->loadParametersBody();

        $this->assertDoesNotMatchRegularExpression(
            "/'\{\\\$holder->resourceName\}'/",
            $body,
            'loadParameters() must not interpolate $holder->resourceName into SQL'
        );
        $this->assertDoesNotMatchRegularExpression(
            "/'\{\\\$holder->schemaName\}'/",
            $body,
            'loadParameters() must not interpolate $holder->schemaName into SQL'
        );
        $this->assertMatchesRegularExpression(
            '/:(name|routineName)\b/',
            $body,
            'loadParameters() must use a :name (or :routineName) named placeholder'
        );
    }
}
