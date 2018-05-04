<?php

namespace KikCmsCore\Services;

use DateTime;
use KikCmsCore\Classes\Model;
use KikCmsCore\Config\DbConfig;
use KikCmsCore\Classes\ObjectList;
use KikCmsCore\Classes\ObjectMap;
use KikCmsCore\Exceptions\DbForeignKeyDeleteException;
use Phalcon\Db;
use Phalcon\Db\ResultInterface;
use Phalcon\Di\Injectable;
use Phalcon\Mvc\Model\Query\Builder;
use Phalcon\Mvc\Model\Resultset;

/**
 * Adds convenience functions to Phalcon's Db Handling
 */
class DbService extends Injectable
{
    /**
     * @param string $model
     * @param array $where
     * @return bool
     * @throws \Exception
     */
    public function delete(string $model, array $where)
    {
        $table       = $this->getTableForModel($model);
        $whereClause = $this->getWhereClauseByArray($where);

        if (empty($whereClause)) {
            return true;
        }

        try {
            return $this->db->delete($table, $whereClause);
        } catch (\Exception $e) {
            if ($e->errorInfo[1] == DbConfig::ERROR_CODE_FK_CONSTRAINT_FAIL) {
                throw new DbForeignKeyDeleteException();
            } else {
                throw $e;
            }
        }
    }

    /**
     * @param string $value
     * @return string
     */
    public function escape(string $value): string
    {
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

        $result->setFetchMode(Db::FETCH_ASSOC);
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

        $result->setFetchMode(Db::FETCH_KEY_PAIR);
        return $result->fetchAll();
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
     *
     * @return int
     */
    public function insert(string $model, array $insert): int
    {
        $table = $this->getTableForModel($model);

        $this->db->insert($table, array_values($insert), array_keys($insert));

        return $this->db->lastInsertId();
    }

    /**
     * @param string $model
     * @param array $insertData
     *
     * @return bool
     */
    public function insertBulk(string $model, array $insertData): bool
    {
        if (empty($insertData)) {
            return true;
        }

        $keys = array_keys($insertData[0]);

        $insertDataChunks = array_chunk($insertData, 1000);

        $this->db->begin();

        foreach ($insertDataChunks as $dataChunk) {
            $insertValues = [];

            foreach ($dataChunk as $row) {
                $row = array_map(function ($value) {
                    return $this->escape($value);
                }, $row);

                $insertValues[] = '(' . implode(',', $row) . ')';
            }

            $this->db->query(
                "INSERT" . " INTO " . $this->getTableForModel($model) . " (" . implode(',', $keys) . ")" .
                "VALUES " . implode(',', $insertValues)
            );
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
     * @param Builder $query
     * @return array
     */
    public function getAssoc(Builder $query): array
    {
        $columns = (array) $query->getColumns();

        if (count($columns) !== 2) {
            throw new \InvalidArgumentException('The query must request two columns');
        }

        $results = $query->getQuery()->execute()->toArray();
        $map     = [];

        foreach ($results as $i => $row) {
            $row                        = (array) $row;
            $map[array_values($row)[0]] = array_values($row)[1];
        }

        return $map;
    }

    /**
     * Retrieve DateTime value from the given query
     *
     * @param Builder $query
     * @return DateTime|null
     */
    public function getDate(Builder $query): ?DateTime
    {
        $value = $this->getValue($query);

        if ( ! $value) {
            return null;
        }

        return new DateTime($value);
    }

    /**
     * @param Builder $query
     * @return bool
     */
    public function getExists(Builder $query): bool
    {
        if (count($query->getQuery()->execute())) {
            return true;
        }

        return false;
    }

    /**
     * Retrieve a single result from the given query
     *
     * @param Builder $query
     * @return null|string
     */
    public function getValue(Builder $query): ?string
    {
        $columns = (array) $query->getColumns();

        if (count($columns) !== 1) {
            throw new \InvalidArgumentException('The query must request a single column');
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
     * @param Builder $query
     * @return array
     */
    public function getValues(Builder $query): array
    {
        $columns = (array) $query->getColumns();

        if (count($columns) !== 1) {
            throw new \InvalidArgumentException('The query must request a single column');
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
     * @param Builder $query
     * @return array
     */
    public function getRow(Builder $query): array
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
     *          'name'     => 'Justin',
     *          'email'    => 'justin@justin.com',
     *      ],
     *      26 => [
     *          'name'     => 'Pete',
     *          'email'    => 'pete@pete.com',
     *      ]
     * ]
     *
     * @param Builder $query
     * @return array
     */
    public function getKeyedRows(Builder $query): array
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
     * @param Builder $query
     * @return array
     */
    public function getKeyedValues(Builder $query): array
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

            $keyedValues[$firstValue][] = $secondValue;
        }

        return $keyedValues;
    }

    /**
     * @param $query
     *
     * @return null|Model|mixed
     */
    public function getObject(Builder $query): ?Model
    {
        if ( ! $object = $query->getQuery()->execute()->getFirst()) {
            return null;
        }

        return $object;
    }

    /**
     * @param Builder $query
     * @return Resultset
     */
    public function getObjects(Builder $query): Resultset
    {
        return $query->getQuery()->execute();
    }

    /**
     * @param Builder $query
     * @param string $class
     * @return ObjectList|mixed
     */
    public function getObjectList(Builder $query, string $class): ObjectList
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
     * @param Builder $query
     * @param string $class
     * @param string $mapBy
     * @return ObjectMap|mixed
     */
    public function getObjectMap(Builder $query, string $class, string $mapBy = 'id'): ObjectMap
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
     * @param Builder $query
     * @return array
     */
    public function getRows(Builder $query): array
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
     *      1 => [
     *          15 => [
     *              'group_id' => 1,
     *              'age'      => 15,
     *              'name'     => 'Justin',
     *              'email'    => 'justin@justin.com',
     *          ],
     *      ],
     *      2 => [
     *          16 => [
     *              'group_id' => 2,
     *              'age'      => 16,
     *              'name'     => 'Pete',
     *              'email'    => 'pete@pete.com',
     *          ]
     *      ]
     * ]
     *
     * @param Builder $query
     * @return array
     */
    public function getTable(Builder $query): array
    {
        $rows  = $this->getRows($query);
        $table = [];

        foreach ($rows as $row) {
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

        if (is_array($value) || is_object($value)) {
            return json_encode($value);
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