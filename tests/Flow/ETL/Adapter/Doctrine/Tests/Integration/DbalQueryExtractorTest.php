<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use Flow\ETL\Adapter\Doctrine\DbalBulkLoader;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Doctrine\Tests\Double\Stub\ArrayExtractor;
use Flow\ETL\Adapter\Doctrine\Tests\Double\Stub\TransformTestData;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\ETL;

final class DbalQueryExtractorTest extends IntegrationTestCase
{
    public function test_extracting_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_extractor_test');

        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            DbalBulkLoader::insert($this->pgsqlDatabaseContext->connection(), $bulkSize = 10, $table)
        )->run();

        $rows = ETL::extract(
            DbalQueryExtractor::single(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id"
            )
        )->fetch();

        $this->assertSame(
            [
                ['row' => ['id' => 1, 'name' => 'Name One', 'description' => 'Description One']],
                ['row' => ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two']],
                ['row' => ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three']],
            ],
            $rows->toArray()
        );
    }

    public function test_extracting_multiple_rows_multiple_times() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_extractor_test');

        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 2, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 3, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 4, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 5, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 6, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 7, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 8, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 9, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 10, 'name' => 'Name', 'description' => 'Description'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            DbalBulkLoader::insert($this->pgsqlDatabaseContext->connection(), $bulkSize = 10, $table)
        )->run();

        $rows = ETL::extract(
            new DbalQueryExtractor(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
                new ParametersSet(
                    ['limit' => 2, 'offset' => 0],
                    ['limit' => 2, 'offset' => 2],
                    ['limit' => 2, 'offset' => 4],
                    ['limit' => 2, 'offset' => 6],
                    ['limit' => 2, 'offset' => 8],
                )
            )
        )->fetch();

        $this->assertSame(10, $rows->count());
        $this->assertSame(
            [
                ['row' => ['id' => 1, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 2, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 3, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 4, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 5, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 6, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 7, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 8, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 9, 'name' => 'Name', 'description' => 'Description']],
                ['row' => ['id' => 10, 'name' => 'Name', 'description' => 'Description']],
            ],
            $rows->toArray()
        );
    }
}
