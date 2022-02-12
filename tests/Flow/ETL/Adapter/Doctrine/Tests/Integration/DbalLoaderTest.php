<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\Tests\Double\Stub\ArrayExtractor;
use Flow\ETL\Adapter\Doctrine\Tests\Double\Stub\TransformTestData;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\ETL;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;

final class DbalLoaderTest extends IntegrationTestCase
{
    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_loader_test');

        $loader = new DbalLoader($table, $bulkSize = 10, $this->connectionParams());

        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load($loader)
        ->run();

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_multiple_rows_at_once_using_existing_connection() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_loader_test');

        $loader = DbalLoader::fromConnection($this->pgsqlDatabaseContext->connection(), $table, $bulkSize = 10, $this->connectionParams());

        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load($loader)
            ->run();

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(1, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
    }

    public function test_inserts_multiple_rows_at_once_after_serialization_and_deserialization() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_loader_test');

        $serializer = new CompressingSerializer(new NativePHPSerializer());
        $loaderSerialized = $serializer->serialize(new DbalLoader($table, $bulkSize = 10, $this->connectionParams()));

        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load($serializer->unserialize($loaderSerialized))
            ->run();

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_multiple_rows_in_two_insert_queries() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_loader_test');

        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            new DbalLoader($table, $bulkSize = 10, $this->connectionParams())
        )->run();

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_new_rows_and_skip_already_existed() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_loader_test');
        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            new DbalLoader($table, $bulkSize = 10, $this->connectionParams())
        )->run();

        ETL::extract(
            new ArrayExtractor(
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            new DbalLoader($table, $bulkSize = 10, $this->connectionParams(), ['skip_conflicts' => true])
        )->run();

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_primary_key() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_loader_test');
        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            new DbalLoader($table, $bulkSize = 10, $this->connectionParams())
        )->run();

        ETL::extract(
            new ArrayExtractor(
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            new DbalLoader($table, $bulkSize = 10, $this->connectionParams(), ['constraint' => 'flow_dbal_loader_test_pkey'])
        )->run();

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }
}
