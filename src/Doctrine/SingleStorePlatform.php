<?php

namespace SingleStore\Laravel\Doctrine;

use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Schema\Identifier;
use Doctrine\DBAL\Schema\TableDiff;
use SingleStore\Laravel\Exceptions\UnsupportedFunctionException;

class SingleStorePlatform extends MySQL80Platform
{
    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff)
    {
        $columnSql  = [];
        $queryParts = [];

        foreach ($diff->addedColumns as $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $columnArray            = $column->toArray();
            $columnArray['comment'] = $this->getColumnComment($column);
            $queryParts[]           = 'ADD ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->removedColumns as $column) {
            if ($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }

            $queryParts[] =  'DROP ' . $column->getQuotedName($this);
        }

        foreach ($diff->changedColumns as $columnDiff) {
            if ($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)) {
                continue;
            }
            $column      = $columnDiff->column;
            $columnArray = $column->toArray();

            
            if($columnDiff->hasNotNullChanged() && $column->getNotnull() == true) {
                throw new UnsupportedFunctionException('Changing a nullable column to not nullable is not supported');
            }

            // Don't propagate default value changes for unsupported column types.
            if ($columnDiff->hasDefaultChanged() &&
            count($columnDiff->changedProperties) === 1 &&
            ($columnArray['type'] instanceof TextType || $columnArray['type'] instanceof BlobType)
            ) {
                continue;
            }

            $columnArray['comment'] = $this->getColumnComment($column);
            $queryParts[]           =  'MODIFY '
            . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->renamedColumns as $oldColumnName => $column) {
            if ($this->onSchemaAlterTableRenameColumn($oldColumnName, $column, $diff, $columnSql)) {
                continue;
            }

            $oldColumnName          = new Identifier($oldColumnName);
            $columnArray            = $column->toArray();
            $columnArray['comment'] = $this->getColumnComment($column);
            $queryParts[]           =  'CHANGE ' . $oldColumnName->getQuotedName($this) . ' ' . $column->getQuotedName($this);
        }

        if (isset($diff->addedIndexes['primary'])) {
            $keyColumns   = array_unique(array_values($diff->addedIndexes['primary']->getColumns()));
            $queryParts[] = 'ADD PRIMARY KEY (' . implode(', ', $keyColumns) . ')';
            unset($diff->addedIndexes['primary']);
        } elseif (isset($diff->changedIndexes['primary'])) {
            // Necessary in case the new primary key includes a new auto_increment column
            foreach ($diff->changedIndexes['primary']->getColumns() as $columnName) {
                if (isset($diff->addedColumns[$columnName]) && $diff->addedColumns[$columnName]->getAutoincrement()) {
                    $keyColumns   = array_unique(array_values($diff->changedIndexes['primary']->getColumns()));
                    $queryParts[] = 'DROP PRIMARY KEY';
                    $queryParts[] = 'ADD PRIMARY KEY (' . implode(', ', $keyColumns) . ')';
                    unset($diff->changedIndexes['primary']);
                    break;
                }
            }
        }

        $sql      = [];
        $tableSql = [];

        if (! $this->onSchemaAlterTable($diff, $tableSql)) {
            if (count($queryParts) > 0) {
                $sql[] = 'ALTER TABLE ' . $diff->getName($this)->getQuotedName($this) . ' ' . implode(', ', $queryParts);
            }
            $sql = array_merge(
                $this->getPreAlterTableIndexForeignKeySQL($diff),
                $sql,
                $this->getPostAlterTableIndexForeignKeySQL($diff)
            );
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * Obtains DBMS specific SQL code portion needed to declare a generic type
     * field to be used in statements like CREATE TABLE.
     *
     * @param string  $name  The name the field to be declared.
     * @param mixed[] $field An associative array with the name of the properties
     *                       of the field being declared as array indexes. Currently, the types
     *                       of supported field properties are as follows:
     *
     *      length
     *          Integer value that determines the maximum length of the text
     *          field. If this argument is missing the field should be
     *          declared to have the longest length allowed by the DBMS.
     *
     *      default
     *          Text value to be used as default for this field.
     *
     *      notnull
     *          Boolean flag that indicates whether this field is constrained
     *          to not be set to null.
     *      charset
     *          Text value with the default CHARACTER SET for this field.
     *      collation
     *          Text value with the default COLLATION for this field.
     *      unique
     *          unique constraint
     *      check
     *          column check constraint
     *      columnDefinition
     *          a string that defines the complete column
     *
     * @return string DBMS specific SQL code portion that should be used to declare the column.
     */
    public function getColumnDeclarationSQL($name, array $field)
    {
        if (isset($field['columnDefinition'])) {
            $columnDef = $this->getCustomTypeDeclarationSQL($field);
        } else {
            $default = $this->getDefaultValueDeclarationSQL($field);

            $charset = isset($field['charset']) && $field['charset'] ?
                ' ' . $this->getColumnCharsetDeclarationSQL($field['charset']) : '';

            $collation = isset($field['collation']) && $field['collation'] ?
                ' ' . $this->getColumnCollationDeclarationSQL($field['collation']) : '';

            $notnull = isset($field['notnull']) && $field['notnull'] ? '' : ' NULL';

            $unique = isset($field['unique']) && $field['unique'] ?
                ' ' . $this->getUniqueFieldDeclarationSQL() : '';

            $check = isset($field['check']) && $field['check'] ?
                ' ' . $field['check'] : '';

            $typeDecl  = $field['type']->getSQLDeclaration($field, $this);
            $columnDef = $typeDecl . $charset . $notnull . $default . $unique . $check . $collation;

            if ($this->supportsInlineColumnComments() && isset($field['comment']) && $field['comment'] !== '') {
                $columnDef .= ' ' . $this->getInlineColumnCommentSQL($field['comment']);
            }
        }

        return $name . ' ' . $columnDef;
    }
}
