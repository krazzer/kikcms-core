<?php

namespace KikCmsCore\Classes;

use Exception;
use KikCmsCore\Config\DbConfig;
use KikCmsCore\Exceptions\DbForeignKeyDeleteException;
use Phalcon\Mvc\Model\Relation;
use Phalcon\Mvc\Model\ResultsetInterface;
use ReflectionClass;
use Phalcon\Mvc\Model\Resultset;

#[\AllowDynamicProperties]
class Model extends \Phalcon\Mvc\Model
{
    const TABLE = null;
    const ALIAS = null;

    public function initialize()
    {
        if ( ! static::TABLE) {
            throw new Exception('const ' . static::class . '::TABLE must be set');
        }

        $this->setSource(static::TABLE);
    }

    /**
     * @return string
     */
    public function getClassName(): string
    {
        return static::class;
    }

    /**
     * @inheritdoc
     */
    public function hasOne($fields, $referenceModel, $referencedFields, $options = null): Relation
    {
        $options = $this->updateDefaults($options);
        return parent::hasOne($fields, $referenceModel, $referencedFields, $options);
    }

    /**
     * @inheritdoc
     */
    public function belongsTo($fields, $referenceModel, $referencedFields, $options = null): Relation
    {
        $options = $this->updateDefaults($options);
        return parent::belongsTo($fields, $referenceModel, $referencedFields, $options);
    }

    /**
     * @inheritdoc
     */
    public function delete(): bool
    {
        try {
            return parent::delete();
        } catch (Exception $e) {
            if (isset($e->errorInfo) && $e->errorInfo[1] == DbConfig::ERROR_CODE_FK_CONSTRAINT_FAIL) {
                throw new DbForeignKeyDeleteException($e->getMessage(), $e->getCode());
            } else {
                throw $e;
            }
        }
    }

    /**
     * @inheritdoc
     */
    public function hasMany($fields, $referenceModel, $referencedFields, $options = null): Relation
    {
        $options = $this->updateDefaults($options);
        return parent::hasMany($fields, $referenceModel, $referencedFields, $options);
    }

    /**
     * @inheritdoc
     */
    public function hasManyToMany($fields, $intermediateModel, $intermediateFields, $intermediateReferencedFields,
        $referenceModel, $referencedFields, $options = null): Relation
    {
        $options = $this->updateDefaults($options);

        return parent::hasManyToMany($fields, $intermediateModel, $intermediateFields, $intermediateReferencedFields,
            $referenceModel, $referencedFields, $options);
    }

    /**
     * @inheritdoc
     *
     * @return Resultset
     */
    public static function find($parameters = null): ResultsetInterface
    {
        /** @var Resultset $resultSet */
        $resultSet = parent::find($parameters);

        return $resultSet;
    }

    /**
     * Alias of find, but will always return an assoc array base on the first two columns of the result
     * typically id => name
     *
     * @param $parameters
     * @return array
     */
    public static function findAssoc($parameters = null)
    {
        $results     = self::find($parameters)->toArray();
        $returnArray = [];

        foreach ($results as $result) {
            $keys = array_keys($result);

            $returnArray[$result[$keys[0]]] = $result[$keys[1]];
        }

        return $returnArray;
    }

    /**
     * @param $id
     *
     * @return null|Model|mixed
     * @noinspection PhpMissingReturnTypeInspection
     */
    public static function getById($id)
    {
        if ( ! $id) {
            return null;
        }

        return self::findFirst([
            'id = :id:',
            'bind' => ['id' => $id]
        ]);
    }

    /**
     * @inheritdoc
     *
     * @return null|Model|mixed
     */
    public static function findFirst($parameters = null): ?Model
    {
        $object = parent::findFirst($parameters);

        if ( ! $object) {
            return null;
        }

        return $object;
    }

    /**
     * @param int[] $ids
     *
     * @return Resultset|array
     */
    public static function getByIdList(array $ids)
    {
        if ( ! $ids) {
            return [];
        }

        return self::find([
            'conditions' => 'id IN ({ids:array})',
            'bind'       => ['ids' => $ids]
        ]);
    }

    /**
     * @param string $name
     *
     * @return null|Model
     */
    public static function getByName(string $name): ?Model
    {
        return self::findFirst([
            "conditions" => "name = ?1",
            "bind"       => [1 => $name]
        ]);
    }

    /**
     * @return array
     */
    public static function getFields()
    {
        $fields = [];

        $oClass    = new ReflectionClass(get_called_class());
        $constants = $oClass->getConstants();

        foreach ($constants as $constant => $value) {
            if (strpos($constant, 'FIELD_') !== false) {
                $fields[] = $value;
            }
        }

        return $fields;
    }

    /**
     * @return array
     */
    public static function getNameList()
    {
        $results = self::find();
        $names   = [];

        foreach ($results as $result) {
            $names[] = $result->name;
        }

        return $names;
    }

    /**
     * @return array
     */
    public static function getNameMap(): array
    {
        $results = self::find(['order' => 'name']);
        $names   = [];

        foreach ($results as $result) {
            $names[$result->id] = $result->name;
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    public function save(): bool
    {
        $this->setRelationDefaults();

        $saved = parent::save();

        if ($messages = $this->getMessages()) {
            foreach ($messages as $message) {
                $message .= ', data: ' . json_encode($this->toArray()) . ', class:' . get_class($this);
                throw new Exception($message);
            }
        }

        return $saved;
    }

    /**
     * Unset a related value, note that this not delete a stored record
     *
     * @param string $alias
     */
    public function unsetRelation(string $alias)
    {
        unset($this->dirtyRelated[strtolower($alias)]);
    }

    /**
     * The defaults option must always fetch results with those default values
     *
     * @param array $options
     * @return array
     */
    private function updateDefaults(array $options): array
    {
        if ( ! isset($options['defaults'])) {
            return $options;
        }

        if (isset($options['params']['conditions']) && $currentCondition = $options['params']['conditions']) {
            $conditions = [$currentCondition];
        } else {
            $conditions = [];
        }

        foreach ($options['defaults'] as $key => $value) {
            $conditions[] = $key . ' = "' . $value . '"';
        }

        $options['params']['conditions'] = implode(' AND ', $conditions);

        return $options;
    }

    /**
     * If a relation has the 'defaults' option, it will check here if those default values are set, and if not, sets them
     */
    private function setRelationDefaults()
    {
        $relations = $this->getModelsManager()->getRelations(get_class($this));

        foreach ($relations as $relation) {
            if ( ! $defaults = $relation->getOption('defaults')) {
                continue;
            }

            $alias = $relation->getOption('alias');

            if ( ! $this->$alias) {
                continue;
            }

            foreach ($defaults as $key => $value) {
                $this->$alias->$key = $value;
            }
        }
    }
}