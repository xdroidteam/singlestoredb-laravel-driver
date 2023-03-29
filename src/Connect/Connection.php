<?php

namespace SingleStore\Laravel\Connect;

use Illuminate\Database\MySqlConnection;
use Doctrine\DBAL\Connection as DoctrineConnection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\MySqlSchemaManager;
use SingleStore\Laravel\Doctrine\SingleStorePlatform;
use SingleStore\Laravel\Query;
use SingleStore\Laravel\QueryGrammar;
use SingleStore\Laravel\Schema;
use SingleStore\Laravel\SchemaBuilder;
use SingleStore\Laravel\SchemaGrammar;

class Connection extends MySqlConnection
{
    /**
     * Get a schema builder instance for the connection.
     *
     * @return SchemaBuilder
     */
    public function getSchemaBuilder()
    {
        if (null === $this->schemaGrammar) {
            $this->useDefaultSchemaGrammar();
        }

        return new Schema\Builder($this);
    }

    /**
     * Get the default query grammar instance.
     *
     * @return QueryGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new Query\Grammar);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return SchemaGrammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new Schema\Grammar);
    }

    /**
     * Get a new query builder instance.
     */
    public function query()
    {
        return new Query\Builder(
            $this,
            $this->getQueryGrammar(),
            $this->getPostProcessor()
        );
    }

    /**
     * Get the Doctrine DBAL schema manager for the connection.
     *
     * @return \Doctrine\DBAL\Schema\AbstractSchemaManager
     */
    public function getDoctrineSchemaManager()
    {
        $singleStorePlatform = new SingleStorePlatform();

        return $this->getCustomSchemaManager($this->getDoctrineConnection(), $singleStorePlatform);
    }

    /**
     * {@inheritdoc}
     *
     * @return MySqlSchemaManager
     */
    public function getCustomSchemaManager(DoctrineConnection $conn, ?AbstractPlatform $platform)
    {
        return new MySqlSchemaManager($conn, $platform);
    }
}
