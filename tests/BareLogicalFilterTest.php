<?php

namespace DreamFactory\Core\Snowflake\Tests;

use DreamFactory\Core\Snowflake\Resources\SnowflakeTable;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

/**
 * Regression for df-core#172: a filter written without parentheses around each
 * condition (a is null OR b is null) must parse the same as the parenthesized
 * form. Previously everything after the first comparison operator was swallowed
 * and the query silently matched the wrong rows.
 *
 * Drives parseFilterString() directly (IS NULL / IS NOT NULL need no value
 * parsing, so no BigQuery connection).
 */
class BareLogicalFilterTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        // The resource imports the root-namespace `Arr` / `DB` aliases that
        // Laravel registers at runtime; register them so the parser resolves
        // outside a booted app (the IS NULL path touches Arr).
        if (!class_exists('Arr', false)) {
            class_alias(\Illuminate\Support\Arr::class, 'Arr');
        }
        if (!class_exists('DB', false)) {
            class_alias(\Illuminate\Support\Facades\DB::class, 'DB');
        }
    }

    protected function parseFilter(string $filter): string
    {
        $table = (new ReflectionClass(SnowflakeTable::class))->newInstanceWithoutConstructor();
        $method = new \ReflectionMethod(SnowflakeTable::class, 'parseFilterString');
        $method->setAccessible(true);

        $fields = [];
        foreach (['a', 'b', 'c'] as $name) {
            $col = new ColumnSchema(['name' => $name]);
            $col->quotedName = '"' . $name . '"';
            $fields[$name] = $col;
        }
        $params = [];

        return $method->invokeArgs($table, [$filter, &$params, $fields, []]);
    }

    public static function cases(): array
    {
        return [
            'OR'        => ['a is null OR b is null'],
            'AND'       => ['a is null AND b is not null'],
            'lowercase' => ['a is null or b is null'],
            'three'     => ['a is null AND b is null OR c is null'],
            'mixed'     => ['(a is null) AND b is null'],
        ];
    }

    /**
     * The bare form must produce the same SQL as the fully parenthesized form.
     */
    #[DataProvider('cases')]
    public function testBareConditionsParseLikeParenthesized(string $bare): void
    {
        $parenthesized = $this->parenthesize($bare);
        $expected = $this->parseFilter($parenthesized);
        $this->assertStringContainsString('"b"', $expected, 'sanity: both fields present in parenthesized form');
        $this->assertSame($expected, $this->parseFilter($bare));
    }

    private function parenthesize(string $bare): string
    {
        // Wrap each top-level condition; good enough for these test inputs.
        $parts = preg_split('/\s+(AND|OR)\s+/i', $bare, -1, PREG_SPLIT_DELIM_CAPTURE);
        $out = '';
        foreach ($parts as $i => $p) {
            if (strcasecmp($p, 'AND') === 0 || strcasecmp($p, 'OR') === 0) {
                $out .= ' ' . strtoupper($p) . ' ';
            } else {
                $p = trim($p);
                $out .= ($p[0] === '(' && substr($p, -1) === ')') ? $p : "($p)";
            }
        }
        return $out;
    }
}
