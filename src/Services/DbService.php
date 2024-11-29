<?php

namespace KikCmsCore\Services;

use AllowDynamicProperties;
use Closure;
use DateTime;
use Exception;
use InvalidArgumentException;
use KikCmsCore\Classes\Model;
use KikCmsCore\Config\DbConfig;
use KikCmsCore\Classes\ObjectList;
use KikCmsCore\Classes\ObjectMap;
use KikCmsCore\Exceptions\DbForeignKeyDeleteException;
use Monolog\Logger;
use Phalcon\Config\Config;
use Phalcon\Db\Enum;
use Phalcon\Db\ResultInterface;
use Phalcon\Di\Injectable;
use Phalcon\Mvc\Model\Query\Builder;
use Phalcon\Mvc\Model\Query\BuilderInterface;
use Phalcon\Mvc\Model\Resultset;
use Phalcon\Mvc\Model\Row;

/**
 * Adds convenience functions to Phalcon's Db Handling
 *
 * @property Logger $logger
 * @property Config $config
 */

#[AllowDynamicProperties]
class DbService extends Injectable
{
    /**
     * @param string $model
     * @param array $where
     * @return bool
     * @throws Exception
     */
    public function delete(string $model, array $where): bool
    {
        $table       = $this->getTableForModel($model);
        $whereClause = $this->getWhereClauseByArray($where);

        if (empty($whereClause)) {
            return true;
        }

        try {
            return $this->db->delete($table, $whereClause);
        } catch (Exception $e) {
            if ($this->config->application->env == 'dev') {
                $this->logger->log(Logger::ERROR, $e);
            }

            if ($e->errorInfo[1] == DbConfig::ERROR_CODE_FK_CONSTRAINT_FAIL) {
                throw new DbForeignKeyDeleteException();
            } else {
                throw $e;
            }
        }
    }

    /**
     * @param string|null $value
     * @return string
     */
    public function escape(?string $value): string
    {
        if ($value === null || $value === '') {
            return 'NULL';
        }

        return $this->db->escapeString($value);
    }

    /**
     * Execute query
     *
     * @param $string
     * @return bool|ResultInterface
     */
    public function query($string)
    {
        return $this->db->query($string);
    }

    /**
     * Returns an array with an assoc array with results per row
     *
     * @param string $query
     * @return array
     */
    public function queryRows(string $query): array
    {
        $result = $this->db->query($query);

        $result->setFetchMode(Enum::FETCH_ASSOC);
        $resultData = $result->fetchAll();

        if ( ! $resultData) {
            return [];
        }

        return $resultData;
    }

    /**
     * Returns an array with an assoc data for one row
     *
     * @param string $query
     * @return array
     */
    public function queryRow(string $query): array
    {
        $result = $this->queryRows($query);

        if ( ! $result) {
            return [];
        }

        return $result[0];
    }

    /**
     * Returns an assoc array where the first two fields are key and value
     *
     * @param string $query
     * @return array
     */
    public function queryAssoc(string $query): array
    {
        $result = $this->db->query($query);

        $result->setFetchMode(Enum::FETCH_KEY_PAIR);
        return $result->fetchAll();
    }

    /**
     * @param string $query
     * @return mixed
     */
    public function queryValue(string $query)
    {
        $result = $this->db->query($query);

        if ($row = $result->fetch()) {
            return $row[0];
        }

        return null;
    }

    /**
     * @param string $query
     * @return array
     */
    public function queryValues(string $query): array
    {
        $values = [];
        $result = $this->db->query($query);

        while ($row = $result->fetch()) {
            $values[] = $row[0];
        }

        return $values;
    }

    /**
     * @param string $model
     * @return bool
     */
    public function truncate(string $model): bool
    {
        return $this->db->delete($this->getTableForModel($model));
    }

    /**
     * @param string $model
     * @param array $set
     * @param mixed $where
     *
     * @return bool
     */
    public function update(string $model, array $set, $where = null): bool
    {
        $table = $this->getTableForModel($model);

        if (is_array($where)) {
            $where = $this->getWhereClauseByArray($where);
        }

        return $this->db->update($table, array_keys($set), array_values($set), $where);
    }

    /**
     * @param string $model
     * @param array $insert
     * @param bool $updateOnDuplicateKey
     * @return int
     */
    public function insert(string $model, array $insert, bool $updateOnDuplicateKey = false): int
    {
        $table = $this->getTableForModel($model);

        if ($updateOnDuplicateKey) {
            $this->db->query($this->getInsertQuery($model, [$insert], true));
        } else {
            $this->db->insert($table, array_values($insert), array_keys($insert));
        }

        return $this->db->lastInsertId();
    }

    /**
     * @param string $model
     * @param array $insertData
     * @param bool $updateOnDuplicateKey
     * @return bool
     */
    public function insertBulk(string $model, array $insertData, bool $updateOnDuplicateKey = false): bool
    {
        if (empty($insertData)) {
            return true;
        }

        $chunks = array_chunk($insertData, 1000);

        $this->db->begin();

        foreach ($chunks as $rows) {
            $this->db->query($this->getInsertQuery($model, $rows, $updateOnDuplicateKey));
        }

        return $this->db->commit();
    }

    /**
     * @param string $model
     * @return null|string
     */
    public function getAliasForModel(string $model): ?string
    {
        /** @var Model $model */
        $model = new $model();

        return $model::ALIAS;
    }

    /**
     * Retrieve a map where the first column is the key, the second is the value
     *
     * @param BuilderInterface $query
     * @return array
     */
    public function getAssoc(BuilderInterface $query): array
    {
        $columns = (array) $query->getColumns();

        if (count($columns) !== 2) {
            throw new InvalidArgumentException('The query must request two columns');
        }

        $results = $query->getQuery()->execute()->toArray();
        $map     = [];

        foreach ($results as $row) {
            $row = (array) $row;

            $map[array_values($row)[0]] = array_values($row)[1];
        }

        return $map;
    }

    /**
     * Retrieve DateTime value from the given query
     *
     * @param BuilderInterface $query
     * @return DateTime|null
     */
    public function getDate(BuilderInterface $query): ?DateTime
    {
        $value = $this->getValue($query);

        if ( ! $value) {
            return null;
        }

        return new DateTime($value);
    }

    /**
     * @param BuilderInterface $query
     * @return bool
     */
    public function getExists(BuilderInterface $query): bool
    {
        if (count($query->getQuery()->execute())) {
            return true;
        }

        return false;
    }

    /**
     * Retrieve a single result from the given query
     *
     * @param BuilderInterface $query
     * @return null|string
     */
    public function getValue(BuilderInterface $query): ?string
    {
        $columns = (array) $query->getColumns();

        if (count($columns) !== 1) {
            throw new InvalidArgumentException('The query must request a single column');
        }

        $result = $query->getQuery()->execute();

        if ( ! count($result)) {
            return null;
        }

        return first($result->getFirst()->toArray());
    }

    /**
     * Retrieve an array with a single column from the given query
     *
     * @param BuilderInterface $query
     * @param bool $ignoreMultipleColumns
     * @return array
     */
    public function getValues(BuilderInterface $query, bool $ignoreMultipleColumns = false): array
    {
        $columns = (array) $query->getColumns();

        if (count($columns) !== 1 && ! $ignoreMultipleColumns) {
            throw new InvalidArgumentException('The query must request a single column');
        }

        $results = $query->getQuery()->execute()->toArray();

        foreach ($results as $i => $row) {
            $results[$i] = first((array) $row);
        }

        return $results;
    }

    /**
     * Retrieve an assoc array with a single row from the given query
     *
     * @param BuilderInterface $query
     * @return array
     */
    public function getRow(BuilderInterface $query): array
    {
        /** @var Model $result */
        $result = $query->getQuery()->execute()->getFirst();

        if ( ! $result) {
            return [];
        }

        return $result->toArray();
    }

    /**
     * Build up a rows with the first value as key from the results of the query, like:
     *
     * $result = [
     *      21 => [
     *          'name'  => 'Justin',
     *          'email' => 'justin@justin.com',
     *      ],
     *      26 => [
     *          'name'  => 'Pete',
     *          'email' => 'pete@pete.com',
     *      ]
     * ]
     *
     * @param BuilderInterface $query
     * @return array
     */
    public function getKeyedRows(BuilderInterface $query): array
    {
        $rows      = $this->getRows($query);
        $keyedRows = [];

        foreach ($rows as $row) {
            $row        = (array) $row;
            $firstValue = first($row);

            unset($row[first_key($row)]);

            $keyedRows[$firstValue] = $row;
        }

        return $keyedRows;
    }

    /**
     * Build up an array like:
     *
     * $result = [
     *      firstColumn => [
     *          secondColumn => thirdColumn,
     *          secondColumn => thirdColumn,
     *      ],
     *      firstColumn => [
     *          secondColumn => thirdColumn,
     *          secondColumn => thirdColumn,
     *      ]
     * ]
     *
     * @param BuilderInterface $query
     * @return array
     */
    public function getKeyedAssoc(BuilderInterface $query): array
    {
        $rows      = $this->getRows($query);
        $keyedRows = [];

        foreach ($rows as $row) {
            if ($row instanceof Row) {
                $row = $row->toArray();
            }

            $row = array_values($row);

            $firstColumn  = $row[0];
            $secondColumn = $row[1];
            $thirdColumn  = $row[2];

            if ( ! array_key_exists($firstColumn, $keyedRows)) {
                $keyedRows[$firstColumn] = [];
            }

            $keyedRows[$firstColumn][$secondColumn] = $thirdColumn;
        }

        return $keyedRows;
    }

    /**
     * Build up an array like:
     *
     * $result = [
     *      firstColumn => [
     *          secondColumn => [
     *              thirdColumn => fourthColumn,
     *              thirdColumn => fourthColumn,
     *          ],
     *          secondColumn => [
     *              thirdColumn => fourthColumn,
     *              thirdColumn => fourthColumn,
     *          ],
     *      ],
     * ]
     *
     * @param BuilderInterface $query
     * @return array
     */
    public function get4dTableAssoc(BuilderInterface $query): array
    {
        $rows      = $this->getRows($query);
        $keyedRows = [];

        foreach ($rows as $row) {
            if ($row instanceof Row) {
                $row = $row->toArray();
            }

            $row = array_values($row);

            $firstColumn  = $row[0];
            $secondColumn = $row[1];
            $thirdColumn  = $row[2];
            $fourthColumn = $row[3];

            if ( ! array_key_exists($firstColumn, $keyedRows)) {
                $keyedRows[$firstColumn] = [];
            }

            if ( ! array_key_exists($secondColumn, $keyedRows[$firstColumn])) {
                $keyedRows[$firstColumn][$secondColumn] = [];
            }

            $keyedRows[$firstColumn][$secondColumn][$thirdColumn] = $fourthColumn;
        }

        return $keyedRows;
    }

    /**
     * Build up an array with the first value as key from the results of the query, and the second as list of values like:
     *
     * $result = [
     *      21 => [
     *          'Justin', 'Pete'
     *      ],
     *      26 => [
     *          'Jessie', 'Hank', 'Walter'
     *      ]
     * ]
     *
     * @param BuilderInterface $query
     * @param bool $removeEmptyValues
     * @return array
     */
    public function getKeyedValues(BuilderInterface $query, bool $removeEmptyValues = false): array
    {
        $rows        = $this->getRows($query);
        $keyedValues = [];

        foreach ($rows as $row) {
            $row         = (array) $row;
            $firstValue  = first($row);
            $secondValue = array_values($row)[1];

            if ( ! array_key_exists($firstValue, $keyedValues)) {
                $keyedValues[$firstValue] = [];
            }

            if ( ! $secondValue && $removeEmptyValues) {
                continue;
            }

            $keyedValues[$firstValue][] = $secondValue;
        }

        return $keyedValues;
    }

    /**
     * @param $query
     *
     * @return null|Model|mixed
     */
    public function getObject(BuilderInterface $query): ?Model
    {
        if ( ! $object = $query->getQuery()->execute()->getFirst()) {
            return null;
        }

        return $object;
    }

    /**
     * @param BuilderInterface $query
     * @return Model[]
     */
    public function getObjects(BuilderInterface $query): array
    {
        $results = $query->getQuery()->execute();

        $objects = [];

        foreach ($results as $result) {
            $objects[] = $result;
        }

        return $objects;
    }

    /**
     * @param BuilderInterface $query
     * @param string $class
     * @return ObjectList|mixed
     */
    public function getObjectList(BuilderInterface $query, string $class = ObjectList::class): ObjectList
    {
        /** @var ObjectList $objectList */
        $objectList = new $class();

        $results = $query->getQuery()->execute();

        foreach ($results as $result) {
            $objectList->add($result);
        }

        return $objectList;
    }

    /**
     * @param BuilderInterface $query
     * @param string $class
     * @param string $mapBy
     * @return ObjectMap|mixed
     */
    public function getObjectMap(BuilderInterface $query, string $class, string $mapBy = 'id'): ObjectMap
    {
        /** @var ObjectMap $objectMap */
        $objectMap = new $class();

        $results = $query->getQuery()->execute();

        foreach ($results as $result) {
            $objectMap->add($result, $result->$mapBy);
        }

        return $objectMap;
    }

    /**
     * Build up an array like:
     *
     * $result = [
     *      firstKey => [
     *          secondKey => [Object, Object, Object, ... ]
     *          secondKey => [Object, Object, Object, ... ]
     *      ],
     *      firstKey => [
     *          secondKey => [Object, Object, Object, ... ]
     *          secondKey => [Object, Object, Object, ... ]
     *      ],
     * ]
     *
     * @param BuilderInterface $query
     * @param string $firstKey
     * @param string $secondKey
     * @return array
     */
    public function getObjectTable(BuilderInterface $query, string $firstKey, string $secondKey): array
    {
        $results = $query->getQuery()->execute();
        $table   = [];

        foreach ($results as $result) {
            $firstKeyValue  = $result->$firstKey;
            $secondKeyValue = $result->$secondKey;

            if ( ! array_key_exists($firstKeyValue, $table)) {
                $table[$firstKeyValue] = [];
            }

            $table[$firstKeyValue][$secondKeyValue][] = $result;
        }

        return $table;
    }

    /**
     * Build up an array like:
     *
     * $result = [
     *      firstKey => [
     *          secondKey => [array, array, array, ... ]
     *          secondKey => [array, array, array, ... ]
     *      ],
     *      firstKey => [
     *          secondKey => [array, array, array, ... ]
     *          secondKey => [array, array, array, ... ]
     *      ],
     * ]
     *
     * @param BuilderInterface $query
     * @return array
     */
    public function getArrayTable(BuilderInterface $query): array
    {
        $results = $query->getQuery()->execute();
        $table   = [];

        foreach ($results as $result) {
            if ($result instanceof Row) {
                $result = $result->toArray();
            }

            $firstColumn  = array_values($result)[0];
            $secondColumn = array_values($result)[1];

            if ( ! array_key_exists($firstColumn, $table)) {
                $table[$firstColumn] = [];
            }

            $table[$firstColumn][$secondColumn][] = $result;
        }

        return $table;
    }

    /**
     * @param BuilderInterface $query
     * @return array
     */
    public function getRows(BuilderInterface $query): array
    {
        return $query->getQuery()->execute()->toArray();
    }

    /**
     * @param string $model
     * @param int $id
     * @return array
     */
    public function getTableRowById(string $model, int $id): array
    {
        $query = (new Builder)
            ->from($model)
            ->where('id = :id:', ['id' => $id]);

        return $this->getRow($query);
    }

    /**
     * @param bool $set
     */
    public function setForeignKeyChecks(bool $set)
    {
        $this->db->query('SET FOREIGN_KEY_CHECKS = ' . (string) ($set ? 1 : 0));
    }

    /**
     * Build up a table from the results of the query, like:
     *
     * $result = [
     *      firstColumn => [
     *          secondColumn => [
     *              'group_id' => 1,
     *              'age'      => 15,
     *              'name'     => 'Justin',
     *              'email'    => 'justin@justin.com',
     *          ],
     *      ],
     *      firstColumn => [
     *          secondColumn => [
     *              'group_id' => 2,
     *              'age'      => 16,
     *              'name'     => 'Pete',
     *              'email'    => 'pete@pete.com',
     *          ]
     *      ]
     * ]
     *
     * @param BuilderInterface $query
     * @return array
     */
    public function getTable(BuilderInterface $query): array
    {
        $rows  = $this->getRows($query);
        $table = [];

        foreach ($rows as $row) {
            if ($row instanceof Row) {
                $row = $row->toArray();
            }

            $firstKey  = array_values($row)[0];
            $secondKey = array_values($row)[1];

            if ( ! array_key_exists($firstKey, $table)) {
                $table[$firstKey] = [];
            }

            $table[$firstKey][$secondKey] = $row;
        }

        return $table;
    }

    /**
     * @param Resultset $results
     * @param string $field
     * @return array
     */
    public function toMap(Resultset $results, string $field): array
    {
        $map = [];

        foreach ($results as $result) {
            $map[$result->$field] = $result;
        }

        return $map;
    }

    /**
     * Format a value so it can be stored in the Db properly
     *
     * @param $value
     * @return mixed
     */
    public function toStorage($value)
    {
        // convert empty string to null
        if ($value === '') {
            return null;
        }

        // json encode objects and arrays
        if (is_array($value) || is_object($value)) {
            return json_encode($value, JSON_NUMERIC_CHECK);
        }

        return $value;
    }

    /**
     * Formats each value of the array using toStorage method
     *
     * @param array $valueMap
     * @return array
     */
    public function toStorageArray(array $valueMap): array
    {
        foreach ($valueMap as $key => $value) {
            $valueMap[$key] = $this->toStorage($value);
        }

        return $valueMap;
    }

    /**
     * @param Closure $action
     * @param bool $throwException
     * @return bool
     * @throws Exception
     */
    public function transaction(Closure $action, $throwException = true): bool
    {
        $this->db->begin();

        try {
            $action();
        } catch (Exception $exception) {
            $this->logger->log(Logger::ERROR, $exception);
            $this->db->rollback();

            if ($throwException) {
                throw $exception;
            }

            return false;
        }

        return $this->db->commit();
    }

    /**
     * @param string $model
     * @param array $rows
     * @param bool $updateOnDuplicateKey
     * @return string
     */
    private function getInsertQuery(string $model, array $rows, bool $updateOnDuplicateKey = false): string
    {
        $keys   = array_keys($rows[0]);
        $values = [];

        foreach ($rows as $row) {
            $values[] = '(' . implode(',', array_map([$this, 'escape'], $row)) . ')';
        }

        $query = "INSERT" . " INTO " . $this->getTableForModel($model) . " (" . implode(',', $keys) . ")" .
            "VALUES " . implode(',', $values);

        if ($updateOnDuplicateKey) {
            $updateStatements = array_map(function ($key) {
                return $key . ' = VALUES(' . $key . ')';
            }, $keys);

            $query .= ' ON DUPLICATE KEY UPDATE ' . implode(', ', $updateStatements);
        }

        return $query;
    }

    /**
     * @param string $model
     * @return string
     */
    private function getTableForModel(string $model): string
    {
        /** @var Model $model */
        $model = new $model();
        return $model->getSource();
    }

    /**
     * @param array $where
     * @return string
     */
    private function getWhereClauseByArray(array $where): string
    {
        $whereClauses = [];

        foreach ($where as $column => $condition) {
            if (is_array($condition)) {
                if ( ! empty($condition)) {
                    $whereClauses[] = $column . " IN (\"" . implode('","', $condition) . "\")";
                }
            } elseif (is_numeric($condition)) {
                $whereClauses[] = $column . ' = ' . $condition;
            } else {
                $whereClauses[] = $column . ' = ' . $this->escape($condition);
            }
        }

        return implode(' AND ', $whereClauses);
    }
}