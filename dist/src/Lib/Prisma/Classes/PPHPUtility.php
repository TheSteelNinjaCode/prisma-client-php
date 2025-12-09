<?php

namespace Lib\Prisma\Classes;

use PP\Validator;
use ReflectionClass;
use Exception;
use PDO;
use UnitEnum;

enum ArrayType: string
{
    case Associative = 'associative';
    case Indexed = 'indexed';
    case Value = 'value';
}

final class PPHPUtility
{
    /**
     * Checks if the fields exist with references in the given selection.
     *
     * @param array $select The selection array containing fields to check.
     * @param array &$relatedEntityFields Reference to an array where related entity fields will be stored.
     * @param array &$primaryEntityFields Reference to an array where primary entity fields will be stored.
     * @param array $relationName An array of relation names.
     * @param array $fields An array of fields in the model.
     * @param string $modelName The name of the model being checked.
     * @param string $timestamp The timestamp field name to be ignored during the check.
     *
     * @throws Exception If a field does not exist in the model or if the selection format is incorrect.
     */
    public static function checkFieldsExistWithReferences(
        array $select,
        array &$relatedEntityFields,
        array &$primaryEntityFields,
        array $relationName,
        array $fields,
        string $modelName,
        string $timestamp
    ) {
        if (isset($select) && is_array($select)) {
            foreach ($select as $key => $value) {
                if ($key === $timestamp) continue;

                if (is_numeric($key) && is_string($value)) {
                    if (array_key_exists($value, $fields))
                        throw new Exception("The '$value' is indexed, waiting example: ['$value' => true]");
                }

                if (isset($value) && empty($value) || !is_bool($value)) {
                    if (is_string($key) && !array_key_exists($key, $fields)) {
                        throw new Exception("The field '$key' does not exist in the $modelName model.");
                    }

                    if (is_string($key) && array_key_exists($key, $fields)) {
                        if (!is_bool($value) && !is_array($value)) {
                            throw new Exception("The '$key' is indexed, waiting example: ['$key' => true]");
                        }
                    }

                    if (!is_array($value))
                        continue;
                }

                if (is_string($key) && is_array($value)) {
                    if (self::isAtomicOperationArray($value)) {
                        continue;
                    }

                    if (isset($value['select'])) {
                        $relatedEntityFields[$key] = $value['select'];
                    } elseif (isset($value['include'])) {
                        $relatedEntityFields[$key] = $value['include'];
                    } else {
                        if (is_array($value) && empty($value)) {
                            $relatedEntityFields[$key] = [$key];
                        } else {
                            if (!is_bool($value) || empty($value)) {
                                throw new Exception("The '$key' is indexed, waiting example: ['$key' => true] or ['$key' => ['select' => ['field1' => true, 'field2' => true]]]");
                            }
                        }
                    }
                } else {
                    foreach (explode(',', $key) as $fieldName) {
                        if ($key === $timestamp || $fieldName === $timestamp) continue;
                        $fieldName = trim($fieldName);

                        if (!array_key_exists($fieldName, $fields)) {
                            $availableFields = implode(', ', array_keys($fields));
                            throw new Exception("The field '$fieldName' does not exist in the $modelName model. Available fields are: $availableFields");
                        }

                        if (
                            in_array($fieldName, $relationName) ||
                            (isset($fields[$fieldName]) && in_array($fields[$fieldName]['type'], $relationName))
                        ) {
                            $relatedEntityFields[$fieldName] = [$fieldName];
                            continue;
                        }

                        $isObject = false;
                        if (isset($fields[$fieldName]) && $fields[$fieldName]['kind'] === 'object') {
                            $isObject = true;
                        }

                        if (!$isObject) {
                            if (in_array($fieldName, $primaryEntityFields)) continue;
                            $primaryEntityFields[] = $fieldName;
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks if the fields in the select array exist in the fields array for the given model.
     *
     * @param array $select The array of fields to select.
     * @param array $fields The array of fields available in the model.
     * @param string $modelName The name of the model being checked.
     *
     * @throws Exception If a field in the select array does not exist in the fields array.
     */
    public static function checkFieldsExist(array $select, array $fields, string $modelName)
    {
        $virtualFields = ['_count', '_max', '_min', '_avg', '_sum'];
        $logicKeys     = ['AND', 'OR', 'NOT'];

        foreach ($select as $key => $value) {

            if (is_string($key) && in_array($key, $logicKeys, true)) {
                if (is_array($value)) {
                    foreach ($value as $sub) {
                        if (is_array($sub)) {
                            self::checkFieldsExist($sub, $fields, $modelName);
                        }
                    }
                }
                continue;
            }

            if (is_numeric($key) && is_string($value)) {
                if (self::fieldExists($key, $fields))
                    throw new Exception("The '$value' is indexed, waiting example: ['$value' => true]");
            }

            if (isset($value) && empty($value) || !is_bool($value)) {
                if (is_string($key) && !self::fieldExists($key, $fields)) {
                    if (in_array($key, $virtualFields, true)) {
                        continue;
                    }
                    throw new Exception("The field '$key' does not exist in the $modelName model.");
                }

                if (is_array($value) && !empty($value)) {
                    if (self::isOperatorArray($value)) {
                        continue;
                    }

                    if (self::isAtomicOperationArray($value)) {
                        continue;
                    }

                    $isRelatedModel = false;
                    foreach ($fields as $field) {
                        $isObject  = ($field['kind'] ?? null) === 'object';
                        $fieldName = $field['name'] ?? null;
                        if ($isObject && $fieldName === $key) {
                            $isRelatedModel = true;
                            break;
                        }
                    }
                    if ($isRelatedModel) continue;

                    $keys = array_keys($value);
                    foreach ($keys as $fieldName) {
                        $fieldName = trim((string)$fieldName);
                        if (!self::fieldExists($fieldName, $fields)) {
                            throw new Exception("The field '$fieldName' does not exist in the $modelName model.");
                        }
                    }
                }

                continue;
            }

            foreach (explode(',', (string)$key) as $fieldName) {
                $fieldName = trim($fieldName);
                if (!self::fieldExists($fieldName, $fields)) {
                    if (in_array($fieldName, $virtualFields, true)) {
                        continue;
                    }
                    throw new Exception("The field '$fieldName' does not exist in the $modelName model.");
                }
            }
        }
    }

    private static function isAtomicOperationArray(array $arr): bool
    {
        $atomicOps = ['increment', 'decrement', 'multiply', 'divide'];
        foreach ($arr as $key => $value) {
            if (in_array($key, $atomicOps, true)) {
                return true;
            }
        }
        return false;
    }

    private static function fieldExists(string $key, array $fields): bool
    {
        foreach ($fields as $field) {
            if (isset($field['name']) && $field['name'] === $key) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks the contents of an array and determines its type.
     *
     * This method iterates through the provided array and checks the type of its elements.
     * It returns an `ArrayType` enum value indicating whether the array is associative,
     * indexed, or contains a single value.
     *
     * @param array $array The array to check.
     * @return ArrayType Returns `ArrayType::Associative` if the array is associative,
     *                   `ArrayType::Indexed` if the array is indexed,
     *                   or `ArrayType::Value` if the array contains a single value.
     */
    public static function checkArrayContents(array $array): ArrayType
    {
        foreach ($array as $key => $value) {
            if (is_array($value)) {
                if (array_keys($value) !== range(0, count($value) - 1)) {
                    return ArrayType::Associative;
                } else {
                    return ArrayType::Indexed;
                }
            } else {
                return ArrayType::Value;
            }
        }
        return ArrayType::Value;
    }

    /**
     * Checks and processes the include array for related entity fields and includes.
     *
     * @param array $include The array of includes to be checked.
     * @param array &$relatedEntityFields The array of related entity fields to be updated.
     * @param array &$includes The array of includes to be updated.
     * @param array $fields The array of fields in the model.
     * @param string $modelName The name of the model being processed.
     *
     * @throws Exception If an include value is indexed incorrectly or if a field does not exist in the model.
     */
    public static function checkIncludes(array $include, array &$relatedEntityFields, array &$includes, array $fields, string $modelName)
    {
        $virtualFields = ['_count', '_max', '_min', '_avg', '_sum'];

        if (isset($include) && is_array($include)) {
            foreach ($include as $key => $value) {
                if (is_array($value) && array_key_exists('join.type', $value)) {
                    continue;
                }

                if (in_array($key, $virtualFields)) {
                    $includes[$key] = $value;
                    continue;
                }

                self::processIncludeValue($key, $value, $relatedEntityFields, $fields, $modelName, $key);

                if (is_numeric($key) && is_string($value)) {
                    throw new Exception("The '$value' is indexed, waiting example: ['$value' => true]");
                }

                if (is_array($value) && array_key_exists($key, $fields)) {
                    $includes[$key] = $value;
                } elseif (is_bool($value)) {
                    $includes[$key] = $value;
                } elseif (!is_array($value)) {
                    throw new Exception("Invalid include format for '$key'. Expecting an array or boolean.");
                }

                if (!array_key_exists($key, $fields)) {
                    throw new Exception("The field '$key' does not exist in the $modelName model.");
                }
            }
        }
    }

    private static function processIncludeValue($key, $value, &$relatedEntityFields, $fields, $modelName, $parentKey)
    {
        if (isset($value['select']) || isset($value['where'])) {
            $relatedEntityFields[$parentKey] = $value;
        } elseif (is_array($value)) {
            if (empty($value)) {
                $relatedEntityFields[$parentKey] = [$parentKey];
            } else {
                foreach ($value as $k => $v) {
                    if (is_string($k) && (is_bool($v) || empty($v))) {
                        $relatedEntityFields[$parentKey]['include'] = [$k => $v];
                    } else {
                        self::processIncludeValue($k, $v, $relatedEntityFields, $fields, $modelName, $parentKey);
                    }
                }
            }
        } else {
            if (!is_bool($value) || empty($value)) {
                throw new Exception("The '$value' is indexed, waiting example: ['$value' => true] or ['$value' => ['select' => ['field1' => true, 'field2' => true]]]");
            }
        }
    }

    /**
     * Processes an array of conditions and converts them into SQL conditions and bindings.
     *
     * @param array $conditions The array of conditions to process.
     * @param array &$sqlConditions The array to store the resulting SQL conditions.
     * @param array &$bindings The array to store the resulting bindings for prepared statements.
     * @param string $dbType The type of the database (e.g., MySQL, PostgreSQL).
     * @param string $tableName The name of the table to which the conditions apply.
     * @param string $prefix The prefix to use for condition keys (used for nested conditions).
     * @param int $level The current level of nesting for conditions (used for recursion).
     *
     * @return void
     */
    public static function processConditions(array $conditions, &$sqlConditions, &$bindings, $dbType, $tableName, $prefix = '', $level = 0)
    {
        foreach ($conditions as $key => $value) {
            if (in_array($key, ['AND', 'OR', 'NOT'])) {
                $groupedConditions = [];
                if ($key === 'NOT') {
                    self::processNotCondition($value, $groupedConditions, $bindings, $dbType, $tableName, $prefix . $key . '_', $level);
                    if (!empty($groupedConditions)) {
                        $conditionGroup = '(' . implode(" $key ", $groupedConditions) . ')';
                        $conditionGroup = 'NOT ' . $conditionGroup;
                        $sqlConditions[] = $conditionGroup;
                    }
                } else {
                    foreach ($value as $conditionKey => $subCondition) {
                        if (is_numeric($conditionKey)) {
                            self::processConditions($subCondition, $groupedConditions, $bindings, $dbType, $tableName, $prefix . $key . $conditionKey . '_', $level + 1);
                        } else {
                            self::processSingleCondition($conditionKey, $subCondition, $groupedConditions, $bindings, $dbType, $tableName, $prefix . $key . $conditionKey . '_', $level + 1);
                        }
                    }
                    if (!empty($groupedConditions)) {
                        $conditionGroup = '(' . implode(" $key ", $groupedConditions) . ')';
                        $sqlConditions[] = $conditionGroup;
                    }
                }
            } else {
                self::processSingleCondition($key, $value, $sqlConditions, $bindings, $dbType, $tableName, $prefix, $level);
            }
        }
    }

    private static function isOperatorArray(array $arr)
    {
        $operators = [
            'contains',
            'startsWith',
            'endsWith',
            'equals',
            'not',
            'gt',
            'gte',
            'lt',
            'lte',
            'in',
            'notIn',
            'increment',
            'decrement',
            'multiply',
            'divide'
        ];
        foreach ($arr as $key => $value) {
            if (!in_array($key, $operators)) {
                return false;
            }
        }
        return true;
    }

    private static function processSingleCondition($key, $value, &$sqlConditions, &$bindings, $dbType, $tableName, $prefix, $level)
    {
        if (is_array($value) && !self::isOperatorArray($value)) {
            foreach ($value as $nestedKey => $nestedValue) {
                self::processSingleCondition(
                    $nestedKey,
                    $nestedValue,
                    $sqlConditions,
                    $bindings,
                    $dbType,
                    $key,
                    $prefix . $key . '_',
                    $level + 1
                );
            }
            return;
        }

        $fieldQuoted = self::quoteColumnName($dbType, $key);
        $qualifiedField = $tableName . '.' . $fieldQuoted;

        if (is_array($value)) {
            foreach ($value as $condition => $val) {

                $enumAllowed = ['equals', 'not', 'in', 'notIn'];
                $unsupported = ['contains', 'startsWith', 'endsWith', 'gt', 'gte', 'lt', 'lte'];

                $castEnum = static function ($v) use ($condition, $key, $enumAllowed, $unsupported) {
                    if ($v instanceof UnitEnum) {
                        if (in_array($condition, $unsupported, true)) {
                            $msg = "Operator '$condition' is not supported for enum field '$key'. ";
                            $msg .= 'Allowed operators: ' . implode(', ', $enumAllowed) . '.';
                            throw new Exception($msg);
                        }
                        return $v->value;
                    }
                    return $v;
                };

                if (in_array($condition, ['in', 'notIn'], true)) {
                    $val = array_map($castEnum, $val);
                } else {
                    $val = $castEnum($val);
                }

                $bindingKey = ":" . $prefix . $key . "_" . $condition . $level;
                switch ($condition) {
                    case 'contains':
                    case 'startsWith':
                    case 'endsWith':
                    case 'equals':
                    case 'not':
                        if ($val === null) {
                            $sqlConditions[] = "$qualifiedField IS NOT NULL";
                        } elseif ($val === '') {
                            $sqlConditions[] = "$qualifiedField != ''";
                        } else {
                            $validatedValue = Validator::string($val, false);
                            $likeOperator = $condition === 'contains' ? ($dbType == 'pgsql' ? 'ILIKE' : 'LIKE') : '=';
                            if ($condition === 'startsWith') $validatedValue .= '%';
                            if ($condition === 'endsWith') $validatedValue = '%' . $validatedValue;
                            if ($condition === 'contains') $validatedValue = '%' . $validatedValue . '%';
                            $sqlConditions[] = $condition === 'not' ? "$qualifiedField != $bindingKey" : "$qualifiedField $likeOperator $bindingKey";
                            $bindings[$bindingKey] = $validatedValue;
                        }
                        break;
                    case 'gt':
                    case 'gte':
                    case 'lt':
                    case 'lte':
                        if (is_float($val)) {
                            $validatedValue = Validator::float($val);
                        } elseif (is_int($val)) {
                            $validatedValue = Validator::int($val);
                        } elseif (strtotime($val) !== false) {
                            $validatedValue = date('Y-m-d H:i:s', strtotime($val));
                        } else {
                            $validatedValue = Validator::string($val, false);
                        }
                        $operator = $condition === 'gt' ? '>' : ($condition === 'gte' ? '>=' : ($condition === 'lt' ? '<' : '<='));
                        $sqlConditions[] = "$qualifiedField $operator $bindingKey";
                        $bindings[$bindingKey] = $validatedValue;
                        break;
                    case 'in':
                    case 'notIn':
                        $inPlaceholders = [];
                        foreach ($val as $i => $inVal) {
                            $inKey = $bindingKey . "_" . $i;
                            $validatedValue = Validator::string($inVal, false);
                            $inPlaceholders[] = $inKey;
                            $bindings[$inKey] = $validatedValue;
                        }
                        $inClause = implode(', ', $inPlaceholders);
                        $sqlConditions[] = "$qualifiedField " . ($condition === 'notIn' ? 'NOT IN' : 'IN') . " ($inClause)";
                        break;
                    default:
                        // Handle other conditions or log an error/warning for unsupported conditions
                        throw new Exception("Unsupported condition: $condition");
                        break;
                }
            }
        } else {
            if ($value === null) {
                $sqlConditions[] = "$qualifiedField IS NULL";
            } elseif ($value === '') {
                $sqlConditions[] = "$qualifiedField = ''";
            } else {
                if ($value instanceof UnitEnum) {
                    $value = $value->value;
                }

                $bindingKey = ":" . $prefix . $key . $level;
                $validatedValue = Validator::string($value, false);
                $sqlConditions[] = "$qualifiedField = $bindingKey";
                $bindings[$bindingKey] = $validatedValue;
            }
        }
    }

    private static function processNotCondition($conditions, &$sqlConditions, &$bindings, $dbType, $tableName, $prefix, $level = 0)
    {
        foreach ($conditions as $key => $value) {
            self::processSingleCondition($key, $value, $sqlConditions, $bindings, $dbType, $tableName, $prefix . 'NOT_', $level);
        }
    }

    /**
     * Checks for invalid keys in the provided data array.
     *
     * This method iterates through the provided data array and checks if each key exists in the allowed fields array.
     * If a key is found that does not exist in the allowed fields, an exception is thrown.
     *
     * @param array $data The data array to check for invalid keys.
     * @param array $fields The array of allowed field names.
     * @param string $modelName The name of the model being checked.
     *
     * @throws Exception If an invalid key is found in the data array.
     */
    public static function checkForInvalidKeys(array $data, array $fields, string $modelName)
    {
        foreach ($data as $key => $value) {
            if (!empty($key) && !in_array($key, $fields)) {
                throw new Exception("The field '$key' does not exist in the $modelName model. Accepted fields: " . implode(', ', $fields));
            }
        }
    }

    public static function queryOptions(
        array  $criteria,
        string &$sql,
        string $dbType,
        string $tableName,
        bool   $addAggregates = true
    ): void {

        if ($addAggregates) {
            $selectParts = [];

            foreach (
                [
                    '_max' => 'MAX',
                    '_min' => 'MIN',
                    '_count' => 'COUNT',
                    '_avg' => 'AVG',
                    '_sum' => 'SUM'
                ] as $key => $func
            ) {

                if (!isset($criteria[$key])) continue;

                foreach ($criteria[$key] as $col => $enabled) {
                    if (!$enabled) continue;
                    $alias = strtolower(substr($key, 1)) . "_$col";
                    $quoted = self::quoteColumnName($dbType, $col);
                    $selectParts[] = "$func($tableName.$quoted) AS $alias";
                }
            }

            if ($selectParts) {
                $sql = str_replace(
                    'SELECT',
                    'SELECT ' . implode(', ', $selectParts) . ',',
                    $sql
                );
            }
        }

        if (isset($criteria['orderBy'])) {
            $parts = self::parseOrderBy($criteria['orderBy'], $dbType, $tableName);
            if ($parts) {
                $sql .= ' ORDER BY ' . implode(', ', $parts);
            }
        }

        if (isset($criteria['take'])) {
            $sql .= ' LIMIT ' . intval($criteria['take']);
        }
        if (isset($criteria['skip'])) {
            $sql .= ' OFFSET ' . intval($criteria['skip']);
        }
    }

    private static function parseOrderBy(
        array  $orderBy,
        string $dbType,
        string $tableName
    ): array {
        $aggKeys = ['_count', '_avg', '_sum', '_min', '_max'];
        $parts   = [];

        foreach ($orderBy as $key => $value) {

            if (in_array($key, $aggKeys, true) && is_array($value)) {
                foreach ($value as $field => $dir) {
                    $alias  = strtolower(substr($key, 1)) . '_' . $field;
                    $quoted = self::quoteColumnName($dbType, $alias);
                    $parts[] = $quoted . ' ' . (strtolower($dir) === 'desc' ? 'DESC' : 'ASC');
                }
                continue;
            }

            if (is_array($value)) {
                foreach ($value as $nested => $dir) {
                    $dir = strtolower($dir) === 'desc' ? 'DESC' : 'ASC';
                    $parts[] = self::quoteColumnName($dbType, $key) . '.' .
                        self::quoteColumnName($dbType, $nested) . " $dir";
                }
            } else {
                $dir = strtolower($value) === 'desc' ? 'DESC' : 'ASC';
                $parts[] = "$tableName." . self::quoteColumnName($dbType, $key) . " $dir";
            }
        }

        return $parts;
    }

    /**
     * Quotes a column name based on the database type.
     *
     * This method adds appropriate quotes around the column name depending on the database type.
     * For PostgreSQL and SQLite, it uses double quotes. For other databases, it uses backticks.
     * If the column name is empty or null, it simply returns an empty string.
     *
     * @param string $dbType The type of the database (e.g., 'pgsql', 'sqlite', 'mysql').
     * @param string|null $column The name of the column to be quoted.
     * @return string The quoted column name or an empty string if the column is null or empty.
     */
    public static function quoteColumnName(string $dbType, ?string $column): string
    {
        if (empty($column)) {
            return '';
        }

        return ($dbType === 'pgsql' || $dbType === 'sqlite') ? "\"$column\"" : "`$column`";
    }

    /**
     * Recursively builds SQL JOIN statements and SELECT fields for nested relations.
     *
     * @param array $include An array of relations to include, with optional nested includes.
     * @param string $parentAlias The alias of the parent table in the SQL query.
     * @param array &$joins An array to collect the generated JOIN statements.
     * @param array &$selectFields An array to collect the generated SELECT fields.
     * @param mixed $pdo The PDO instance for database connection.
     * @param string $dbType The type of the database (e.g., 'mysql', 'pgsql').
     * @param object|null $model The model object containing metadata about the relations.
     *
     * @throws Exception If relation metadata is not defined or if required fields/references are missing.
     */
    public static function buildJoinsRecursively(
        array $include,
        string $parentAlias,
        array &$joins,
        array &$selectFields,
        PDO $pdo,
        string $dbType,
        ?object $model = null,
        string $defaultJoinType = 'INNER JOIN',
        string $pathPrefix = ''
    ) {
        foreach ($include as $relationName => $relationOptions) {
            $joinType = isset($relationOptions['join.type'])
                ? strtoupper($relationOptions['join.type']) . ' JOIN'
                : $defaultJoinType;

            if (!in_array($joinType, ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN'], true)) {
                throw new Exception("Invalid join type: $joinType (expected 'INNER JOIN', 'LEFT JOIN', or 'RIGHT JOIN')");
            }

            // Extract nested includes
            $nestedInclude = [];
            if (is_array($relationOptions) && isset($relationOptions['include']) && is_array($relationOptions['include'])) {
                $nestedInclude = $relationOptions['include'];
            }
            $isNested = !empty($nestedInclude);

            // 1. Fetch metadata
            if (!isset($model->_fields[$relationName])) {
                throw new Exception("Relation metadata not defined for '$relationName' in " . get_class($model));
            }

            // 2. Identify related class
            $relatedClassName = "Lib\\Prisma\\Classes\\" . $model->_fields[$relationName]['type'] ?? null;
            $relatedClass = new $relatedClassName($pdo);
            if (!$relatedClass) {
                throw new Exception("Could not instantiate class for relation '$relationName'.");
            }

            // 3. Determine DB table
            $joinTable = $relatedClass->_tableName ?? null;
            if (!$joinTable) {
                throw new Exception("No valid table name found for relation '$relationName'.");
            }

            $newAliasQuoted = PPHPUtility::quoteColumnName($dbType, $relationName);

            // 5. Build the ON condition
            $joinConditions = [];
            $fieldsRelatedWithKeys = $model->_fieldsRelatedWithKeys[$relationName] ?? null;
            if ($fieldsRelatedWithKeys) {
                $relationToFields = $fieldsRelatedWithKeys['relationToFields'] ?? [];
                $relationFromFields = $fieldsRelatedWithKeys['relationFromFields'] ?? [];

                if (count($relationToFields) !== count($relationFromFields)) {
                    throw new Exception("Mismatched 'references' and 'fields' for '$relationName'.");
                }

                foreach ($relationToFields as $index => $toField) {
                    $fromField = $relationFromFields[$index] ?? null;
                    if (!$toField || !$fromField) {
                        throw new Exception("Missing references/fields for '$relationName' at index $index.");
                    }

                    $fromFieldExists = array_key_exists($fromField, $model->_fields);

                    if ($fromFieldExists) {
                        $joinConditions[] = sprintf(
                            '%s.%s = %s.%s',
                            $parentAlias,
                            PPHPUtility::quoteColumnName($dbType, $fromField),
                            $newAliasQuoted,
                            PPHPUtility::quoteColumnName($dbType, $toField)
                        );
                    } else {
                        $joinConditions[] = sprintf(
                            '%s.%s = %s.%s',
                            $parentAlias,
                            PPHPUtility::quoteColumnName($dbType, $toField),
                            $newAliasQuoted,
                            PPHPUtility::quoteColumnName($dbType, $fromField)
                        );
                    }
                }
            } else {
                throw new Exception("Relation '$relationName' not properly defined.");
            }

            $joinCondition = implode(' AND ', $joinConditions);

            // 6. Add the JOIN statement
            $joinTableQuoted = PPHPUtility::quoteColumnName($dbType, $joinTable);
            $joins[] = sprintf(
                '%s %s AS %s ON %s',
                $joinType,
                $joinTableQuoted,
                $newAliasQuoted,
                $joinCondition
            );

            // 7. ADD COLUMNS (with the *full path prefix*).
            //    e.g. if pathPrefix="" and relationName="post", then childPathPrefix="post".
            //         if pathPrefix="post" and relationName="categories", => "post.categories".
            $childPathPrefix = $pathPrefix
                ? $pathPrefix . '.' . $relationName
                : $relationName;

            $fieldsOnly = $relatedClass->_fieldsOnly ?? [];
            foreach ($fieldsOnly as $field) {
                $quotedField       = PPHPUtility::quoteColumnName($dbType, $field);
                $columnAlias       = $childPathPrefix . '.' . $field;      // e.g. "post.categories.id"
                $columnAliasQuoted = PPHPUtility::quoteColumnName($dbType, $columnAlias);

                $selectFields[] = sprintf(
                    '%s.%s AS %s',
                    $newAliasQuoted,
                    $quotedField,
                    $columnAliasQuoted
                );
            }

            // 8. Recurse for nested includes
            if ($isNested) {
                self::buildJoinsRecursively(
                    $nestedInclude,
                    $newAliasQuoted,   // use this for the next level's JOIN
                    $joins,
                    $selectFields,
                    $pdo,
                    $dbType,
                    $relatedClass,
                    $defaultJoinType,
                    $childPathPrefix   // pass down the updated path
                );
            }
        }
    }

    public static function compareStringsAlphabetically($string1, $string2)
    {
        $lowerString1 = strtolower($string1);
        $lowerString2 = strtolower($string2);

        if ($lowerString1 < $lowerString2) {
            return [
                'A' => $string1,
                'B' => $string2,
                'Name' => "_" . ucfirst($string1) . "To" . ucfirst($string2)
            ];
        } else {
            return [
                'A' => $string2,
                'B' => $string1,
                'Name' => "_" . ucfirst($string2) . "To" . ucfirst($string1)
            ];
        }
    }

    private static function handleImplicitRelationSelect(
        string $model,
        string $relatedModel,
        string $dbType,
        PDO $pdo,
        mixed $primaryId
    ): array {
        $implicitModelInfo = PPHPUtility::compareStringsAlphabetically($relatedModel, $model);
        $searchColumn = ($relatedModel === $implicitModelInfo['A']) ? 'B' : 'A';
        $tableName = self::quoteColumnName($dbType, $implicitModelInfo['Name']);
        $searchColumnQuoted = self::quoteColumnName($dbType, $searchColumn);

        $sqlSelect = "SELECT * FROM $tableName WHERE $searchColumnQuoted = ?";
        $stmtSelect = $pdo->prepare($sqlSelect);
        $stmtSelect->execute([$primaryId]);
        return $stmtSelect->fetchAll();
    }

    private static function handleImplicitRelationInsert(
        string $model,
        string $relatedModel,
        string $dbType,
        PDO $pdo,
        mixed $primaryId,
        mixed $relatedId
    ): array {
        $implicitModelInfo = PPHPUtility::compareStringsAlphabetically($relatedModel, $model);
        $searchColumn = ($relatedModel === $implicitModelInfo['A']) ? 'B' : 'A';
        $returnColumn = ($searchColumn === 'A') ? 'B' : 'A';

        if ($implicitModelInfo['A'] === $model) {
            $searchColumnValue = $primaryId;
            $returnColumnValue = $relatedId;
        } else {
            $searchColumnValue = $relatedId;
            $returnColumnValue = $primaryId;
        }

        $tableName = self::quoteColumnName($dbType, $implicitModelInfo['Name']);
        $searchColumnQuoted = self::quoteColumnName($dbType, $searchColumn);
        $returnColumnQuoted = self::quoteColumnName($dbType, $returnColumn);

        $sql = "INSERT IGNORE INTO $tableName ($searchColumnQuoted, $returnColumnQuoted) VALUES (?, ?)";
        $stmt = $pdo->prepare($sql);
        $stmt->execute([$searchColumnValue, $returnColumnValue]);

        $sqlSelect = "SELECT * FROM $tableName WHERE $searchColumnQuoted = ? AND $returnColumnQuoted = ?";
        $stmtSelect = $pdo->prepare($sqlSelect);
        $stmtSelect->execute([$searchColumnValue, $returnColumnValue]);

        $result = $stmtSelect->fetch();
        return $result ?: [];
    }

    private static function handleImplicitRelationDelete(
        string $model,
        string $relatedModel,
        string $dbType,
        PDO $pdo,
        mixed $primaryId,
        array $allNewRelatedIds
    ): void {
        $implicitModelInfo = PPHPUtility::compareStringsAlphabetically($relatedModel, $model);
        $searchColumn = ($relatedModel === $implicitModelInfo['A']) ? 'B' : 'A';
        $returnColumn = ($searchColumn === 'A') ? 'B' : 'A';

        $tableName = self::quoteColumnName($dbType, $implicitModelInfo['Name']);
        $searchColumnQuoted = self::quoteColumnName($dbType, $searchColumn);
        $returnColumnQuoted = self::quoteColumnName($dbType, $returnColumn);

        if (count($allNewRelatedIds) > 0) {
            $placeholders = implode(',', array_fill(0, count($allNewRelatedIds), '?'));
            $sqlDelete = "DELETE FROM $tableName WHERE $searchColumnQuoted = ? AND $returnColumnQuoted NOT IN ($placeholders)";
            $stmtDelete = $pdo->prepare($sqlDelete);
            $stmtDelete->execute(array_merge([$primaryId], $allNewRelatedIds));
        } else {
            $sqlDelete = "DELETE FROM $tableName WHERE $searchColumnQuoted = ?";
            $stmtDelete = $pdo->prepare($sqlDelete);
            $stmtDelete->execute([$primaryId]);
        }
    }

    public static function processRelation(
        string $modelName,
        string $relatedFieldName,
        array  $fieldData,
        PDO    $pdo,
        string $dbType,
        bool   $requestOption = true,
    ): array {
        $modelClassName  = "Lib\\Prisma\\Classes\\{$modelName}";
        $modelClass      = (new ReflectionClass($modelClassName))->newInstance($pdo);

        $modelFieldsRelatedWithKeys = $modelClass->_fieldsRelatedWithKeys[$relatedFieldName];
        $modelRelatedFromFields     = $modelFieldsRelatedWithKeys['relationFromFields'];
        $modelRelatedToFields       = $modelFieldsRelatedWithKeys['relationToFields'];

        $modelRelatedField       = $modelClass->_fields[$relatedFieldName];
        $modelRelatedFieldIsList = $modelRelatedField['isList'] ?? false;
        $modelRelatedFieldType   = $modelRelatedField['type'];

        $relatedClassName = "Lib\\Prisma\\Classes\\{$modelRelatedFieldType}";
        $relatedClass     = (new ReflectionClass($relatedClassName))->newInstance($pdo);

        $inverseInfo      = null;
        $childFkFields    = [];
        foreach ($relatedClass->_fieldsRelatedWithKeys as $childField => $info) {
            $infoType = $info['type'] ?? ($relatedClass->_fields[$childField]['type'] ?? null);
            if ($infoType === $modelName && !empty($info['relationFromFields'])) {
                $inverseInfo   = $info;
                $childFkFields = $info['relationFromFields'];
                break;
            }
        }
        $isExplicitOneToMany = $modelRelatedFieldIsList && $inverseInfo !== null;

        $relatedResult = null;

        foreach ($fieldData as $action => $actionData) {
            $operations = isset($actionData[0]) ? $actionData : [$actionData];

            foreach ($operations as $op) {
                switch ($action) {
                    case 'connect':
                        if ($isExplicitOneToMany) {
                            $parentId = $op[$childFkFields[0]]
                                ?? throw new Exception("Missing parent id while connecting '{$relatedFieldName}'.");

                            $where = array_diff_key($op, array_flip($childFkFields));
                            if (!$where) {
                                throw new Exception("A unique selector (e.g. 'code') is required inside 'connect' for '{$relatedFieldName}'.");
                            }
                            $fkUpdate      = array_fill_keys($childFkFields, $parentId);
                            $relatedResult = $relatedClass->update([
                                'where' => $where,
                                'data'  => $fkUpdate,
                            ]);
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $relatedFieldData = $op[$modelRelatedFieldType];
                            $modelFieldData   = $op[$modelName];

                            if (!isset($relatedFieldData[$relatedClass->_primaryKey])) {
                                $existing = $relatedClass->findUnique(['where' => $relatedFieldData]);
                                if (!$existing) {
                                    throw new Exception("Cannot connect '{$relatedFieldName}': related record not found.");
                                }
                                $relatedFieldData[$relatedClass->_primaryKey] = $existing->{$relatedClass->_primaryKey};
                            }

                            if (!isset($modelFieldData[$modelClass->_primaryKey])) {
                                $existingParent = $modelClass->findUnique(['where' => $modelFieldData]);
                                if (!$existingParent) {
                                    throw new Exception("Cannot connect '{$relatedFieldName}': parent record not found.");
                                }
                                $modelFieldData[$modelClass->_primaryKey] = $existingParent->{$modelClass->_primaryKey};
                            }

                            $relatedId = $relatedFieldData[$relatedClass->_primaryKey];
                            $modelId   = $modelFieldData[$modelClass->_primaryKey];

                            $implicit  = self::compareStringsAlphabetically($modelRelatedFieldType, $modelName);

                            if ($implicit['A'] === $modelName) {
                                $idA = $modelId;
                                $idB = $relatedId;
                            } else {
                                $idA = $relatedId;
                                $idB = $modelId;
                            }

                            $relatedResult = self::handleImplicitRelationInsert(
                                $modelName,
                                $modelRelatedFieldType,
                                $dbType,
                                $pdo,
                                $idA,
                                $idB
                            );
                        } else {
                            if (!$modelRelatedFieldIsList && count($operations) > 1) {
                                throw new Exception("Cannot connect multiple records for a non-list relation '{$relatedFieldName}'.");
                            }
                            $relatedResult = $relatedClass->findUnique(['where' => $op]);
                        }
                        break;
                    case 'connectOrCreate':
                        if (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $relatedFieldData = $op[$modelRelatedFieldType];
                            $modelFieldData = $op[$modelName];
                            $existingRecord = $relatedClass->findFirst(['where' => $relatedFieldData['where']]);

                            if ($existingRecord) {
                                $record = $existingRecord;
                            } else {
                                $record = $relatedClass->create(['data' => $relatedFieldData['create']]);
                            }

                            $relatedResult = self::handleImplicitRelationInsert(
                                $modelName,
                                $modelRelatedFieldType,
                                $dbType,
                                $pdo,
                                $modelFieldData[$modelClass->_primaryKey],
                                $record->{$relatedClass->_primaryKey},
                            );
                        } else {

                            if (!$modelRelatedFieldIsList && count($operations) > 1) {
                                throw new Exception("Cannot connectOrCreate multiple records for a non-list relation '$relatedFieldName'.");
                            }

                            $existing = $relatedClass->findUnique(['where' => $op['where']]);

                            if ($existing) {
                                $relatedResult = $existing;
                            } else {
                                $relatedResult = $relatedClass->create(['data' => $op['create']]);
                            }
                        }
                        break;
                    case 'create':
                        if (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $relatedFieldData = $op[$modelRelatedFieldType];
                            $modelFieldData = $op[$modelName];
                            $relatedCreatedData = $relatedClass->create(['data' => $relatedFieldData]);
                            $relatedResult = self::handleImplicitRelationInsert(
                                $modelName,
                                $modelRelatedFieldType,
                                $dbType,
                                $pdo,
                                $modelFieldData[$modelClass->_primaryKey],
                                $relatedCreatedData->{$relatedClass->_primaryKey},
                            );
                        } else {
                            if (!$modelRelatedFieldIsList && count($operations) > 1) {
                                throw new Exception("Cannot create multiple records for a non-list relation '$relatedFieldName'.");
                            }

                            $relatedResult = $relatedClass->create(['data' => $op]);
                        }
                        break;
                    case 'delete':
                        $whereCondition = $op[$modelRelatedFieldType];
                        $relatedResult = $relatedClass->delete(['where' => $whereCondition]);
                        break;
                    case 'deleteMany':
                        if ($isExplicitOneToMany) {
                            foreach ($operations as $opDelete) {
                                if (!isset($opDelete['where'])) {
                                    throw new Exception("deleteMany requires 'where' for '{$relatedFieldName}'.");
                                }

                                $where = $opDelete['where'];

                                if (!empty($childFkFields)) {
                                    $parentId = $opDelete[$childFkFields[0]] ?? null;
                                    if ($parentId !== null) {
                                        $where[$childFkFields[0]] = $parentId;
                                    }
                                }

                                $relatedClass->deleteMany(['where' => $where]);
                            }

                            return [];
                        } else {
                            throw new Exception("deleteMany is only supported for one-to-many relations.");
                        }
                        break;
                    case 'disconnect':
                        if ($isExplicitOneToMany) {
                            foreach ($operations as $opDisc) {
                                $where = array_diff_key($opDisc, array_flip($childFkFields));
                                $relatedClass->update([
                                    'where' => $where,
                                    'data'  => array_fill_keys($childFkFields, null),
                                ]);
                            }
                            $relatedResult = true;
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $rData = $op[$modelRelatedFieldType];
                            $mData = $op[$modelName];
                            $relatedResult = self::handleImplicitRelationDelete(
                                $modelName,
                                $modelRelatedFieldType,
                                $dbType,
                                $pdo,
                                $mData[$modelClass->_primaryKey],
                                $rData[$relatedClass->_primaryKey]
                            );
                        } else {
                            $relatedResult = $relatedClass->delete(['where' => $op]);
                        }
                        break;
                    case 'set':
                        if ($isExplicitOneToMany) {
                            if (empty($operations)) {
                                return [];
                            }

                            $parentId = $operations[0][$childFkFields[0]]
                                ?? throw new Exception("Missing parent id in 'set' for '{$relatedFieldName}'.");

                            $newIds = [];
                            $keyFields = [];

                            $keyFields = array_merge($keyFields, $childFkFields);

                            foreach ($relatedClass->_fields as $fieldName => $fieldMeta) {
                                $kind = $fieldMeta['kind'] ?? 'scalar';
                                $isReadOnly = $fieldMeta['isReadOnly'] ?? false;

                                if ($kind === 'scalar' && $isReadOnly && $fieldName !== 'id') {
                                    $keyFields[] = $fieldName;
                                }
                            }

                            $keyFields = array_unique($keyFields);

                            foreach ($operations as $opSet) {
                                $data = $opSet;

                                $where = [];
                                foreach ($keyFields as $field) {
                                    if (isset($data[$field])) {
                                        $where[$field] = $data[$field];
                                    }
                                }

                                if (empty($where)) {
                                    throw new Exception("Cannot determine unique identifier for '{$relatedFieldName}'. No key fields found.");
                                }

                                $existing = $relatedClass->findFirst(['where' => $where]);

                                if ($existing) {
                                    $relatedClass->update([
                                        'where' => ['id' => $existing->id],
                                        'data' => $data,
                                    ]);
                                    $newIds[] = $existing->id;
                                } else {
                                    $created = $relatedClass->create(['data' => $data]);
                                    $newIds[] = $created->id;
                                }
                            }

                            if (!empty($newIds)) {
                                $deleteWhere = [
                                    $childFkFields[0] => $parentId,
                                    'id' => ['notIn' => $newIds],
                                ];
                            } else {
                                $deleteWhere = [$childFkFields[0] => $parentId];
                            }

                            $relatedClass->deleteMany(['where' => $deleteWhere]);

                            return [];
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $newRelatedIds = [];
                            $primaryId = null;

                            foreach ($operations as $opSet) {
                                $relatedFieldData = $opSet[$modelRelatedFieldType];
                                $modelFieldData   = $opSet[$modelName];
                                $newRelatedIds[]  = $relatedFieldData[$relatedClass->_primaryKey];
                                if (!$primaryId) {
                                    $primaryId = $modelFieldData[$modelClass->_primaryKey];
                                }
                            }
                            $newRelatedIds = array_unique($newRelatedIds);

                            self::handleImplicitRelationDelete(
                                $modelName,
                                $modelRelatedFieldType,
                                $dbType,
                                $pdo,
                                $primaryId,
                                $newRelatedIds
                            );

                            foreach ($newRelatedIds as $relatedId) {
                                self::handleImplicitRelationInsert(
                                    $modelName,
                                    $modelRelatedFieldType,
                                    $dbType,
                                    $pdo,
                                    $primaryId,
                                    $relatedId
                                );
                            }

                            $relatedResult = self::handleImplicitRelationSelect(
                                $modelName,
                                $modelRelatedFieldType,
                                $dbType,
                                $pdo,
                                $primaryId
                            );
                        } else {
                            $relatedResult = $relatedClass->findUnique(['where' => $op]);
                        }
                        break;
                    case 'update':
                        if (!empty($modelRelatedFromFields) && !empty($modelRelatedToFields)) {
                            $relatedResult = $relatedClass->update([
                                'where' => $op['where'],
                                'data' => $op['data']
                            ]);
                        } else {
                            if (!isset($op[$modelRelatedFieldType])) {
                                throw new Exception(
                                    "Expected '{$modelRelatedFieldType}' key in update operation for implicit relation '{$relatedFieldName}'."
                                );
                            }
                            $relatedFieldData = $op[$modelRelatedFieldType];
                            $relatedResult = $relatedClass->update([
                                'where' => $relatedFieldData['where'],
                                'data' => $relatedFieldData['data']
                            ]);
                        }
                        break;
                    case 'updateMany':
                        if ($isExplicitOneToMany) {
                            if (empty($operations)) {
                                return [];
                            }

                            $parentId = $operations[0][$childFkFields[0]] ?? null;
                            if ($parentId === null) {
                                throw new Exception("Missing parent id in 'updateMany' for '{$relatedFieldName}'.");
                            }

                            $fieldUpdates = [];
                            $ids = [];

                            foreach ($operations as $opUpdate) {
                                $where = $opUpdate['where'];
                                $data = $opUpdate['data'];

                                $record = $relatedClass->findFirst([
                                    'where' => array_merge($where, [$childFkFields[0] => $parentId])
                                ]);
                                if (!$record) continue;

                                $ids[] = $record->id;

                                foreach ($data as $field => $value) {
                                    if (!isset($fieldUpdates[$field])) {
                                        $fieldUpdates[$field] = [];
                                    }
                                    $fieldUpdates[$field][$record->id] = $value;
                                }
                            }

                            if (empty($ids)) {
                                return [];
                            }

                            $tableName = PPHPUtility::quoteColumnName($dbType, $relatedClass->_tableName);
                            $idColumn = PPHPUtility::quoteColumnName($dbType, 'id');

                            $setClauses = [];
                            $bindings = [];

                            foreach ($fieldUpdates as $field => $updates) {
                                $fieldQuoted = PPHPUtility::quoteColumnName($dbType, $field);
                                $caseWhen = "CASE";

                                foreach ($updates as $id => $value) {
                                    $placeholder = ":upd_{$field}_" . count($bindings);
                                    $idPlaceholder = ":id_case_" . count($bindings);
                                    $caseWhen .= " WHEN $idColumn = $idPlaceholder THEN $placeholder";
                                    $bindings[$idPlaceholder] = $id;
                                    $bindings[$placeholder] = $value;
                                }

                                $caseWhen .= " ELSE $fieldQuoted END";
                                $setClauses[] = "$fieldQuoted = $caseWhen";
                            }

                            $idPlaceholders = [];
                            foreach ($ids as $id) {
                                $placeholder = ":id_" . count($bindings);
                                $idPlaceholders[] = $placeholder;
                                $bindings[$placeholder] = $id;
                            }

                            $sql = "UPDATE $tableName SET " . implode(', ', $setClauses) .
                                " WHERE $idColumn IN (" . implode(', ', $idPlaceholders) . ")";

                            $stmt = $pdo->prepare($sql);
                            foreach ($bindings as $key => $value) {
                                $stmt->bindValue($key, $value);
                            }
                            $stmt->execute();

                            return [];
                        } else {
                            throw new Exception("updateMany is only supported for one-to-many relations.");
                        }
                        break;
                    case 'upsert':
                        if ($isExplicitOneToMany) {
                            if (empty($operations)) {
                                return [];
                            }

                            $parentId = $operations[0][$childFkFields[0]] ?? null;
                            if ($parentId === null) {
                                throw new Exception("Missing parent id in 'upsert' for '{$relatedFieldName}'.");
                            }

                            $tableName = PPHPUtility::quoteColumnName($dbType, $relatedClass->_tableName);
                            $allFields = [];
                            $values = [];
                            $bindings = [];

                            $sampleOp = $operations[0];
                            $createData = $sampleOp['create'];
                            $createData[$childFkFields[0]] = $parentId;
                            $fields = array_keys($createData);

                            foreach ($fields as $field) {
                                $allFields[] = PPHPUtility::quoteColumnName($dbType, $field);
                            }

                            foreach ($operations as $idx => $opUpsert) {
                                $createData = $opUpsert['create'];
                                $createData[$childFkFields[0]] = $parentId;

                                $rowPlaceholders = [];
                                foreach ($fields as $field) {
                                    $placeholder = ":v{$idx}_{$field}";
                                    $rowPlaceholders[] = $placeholder;
                                    $bindings[$placeholder] = $createData[$field] ?? null;
                                }
                                $values[] = '(' . implode(', ', $rowPlaceholders) . ')';
                            }

                            if ($dbType === 'mysql') {
                                $updateClauses = [];
                                foreach ($fields as $field) {
                                    if ($field !== 'id') {
                                        $fieldQuoted = PPHPUtility::quoteColumnName($dbType, $field);
                                        $updateClauses[] = "$fieldQuoted = VALUES($fieldQuoted)";
                                    }
                                }

                                $sql = "INSERT INTO $tableName (" . implode(', ', $allFields) . ") VALUES " .
                                    implode(', ', $values) .
                                    " ON DUPLICATE KEY UPDATE " . implode(', ', $updateClauses);
                            } elseif ($dbType === 'pgsql') {
                                $updateClauses = [];
                                $conflictFields = [];

                                foreach ($operations[0]['where'] as $whereField => $whereValue) {
                                    $conflictFields[] = PPHPUtility::quoteColumnName($dbType, $whereField);
                                }

                                foreach ($fields as $field) {
                                    if ($field !== 'id' && !in_array($field, array_keys($operations[0]['where']))) {
                                        $fieldQuoted = PPHPUtility::quoteColumnName($dbType, $field);
                                        $updateClauses[] = "$fieldQuoted = EXCLUDED.$fieldQuoted";
                                    }
                                }

                                $sql = "INSERT INTO $tableName (" . implode(', ', $allFields) . ") VALUES " .
                                    implode(', ', $values) .
                                    " ON CONFLICT (" . implode(', ', $conflictFields) . ") DO UPDATE SET " .
                                    implode(', ', $updateClauses);
                            } elseif ($dbType === 'sqlite') {
                                $updateClauses = [];
                                $conflictFields = [];

                                foreach ($operations[0]['where'] as $whereField => $whereValue) {
                                    $conflictFields[] = PPHPUtility::quoteColumnName($dbType, $whereField);
                                }

                                foreach ($fields as $field) {
                                    if ($field !== 'id' && !in_array($field, array_keys($operations[0]['where']))) {
                                        $fieldQuoted = PPHPUtility::quoteColumnName($dbType, $field);
                                        $updateClauses[] = "$fieldQuoted = excluded.$fieldQuoted";
                                    }
                                }

                                $sql = "INSERT INTO $tableName (" . implode(', ', $allFields) . ") VALUES " .
                                    implode(', ', $values) .
                                    " ON CONFLICT (" . implode(', ', $conflictFields) . ") DO UPDATE SET " .
                                    implode(', ', $updateClauses);
                            } else {
                                throw new Exception("Unsupported database type: $dbType");
                            }

                            $stmt = $pdo->prepare($sql);
                            foreach ($bindings as $key => $value) {
                                $stmt->bindValue($key, $value);
                            }
                            $stmt->execute();

                            return [];
                        }
                        break;
                    default:
                        throw new Exception("Unsupported operation '$action' for relation '{$relatedFieldName}'.");
                }
            }
        }

        $relatedResult = (array)$relatedResult;

        if (!$requestOption) {
            return $relatedResult;
        }

        if ($modelRelatedFieldIsList && $isExplicitOneToMany) {
            return [];
        }

        if ($modelRelatedFieldIsList && empty($modelRelatedFromFields)) {
            return [];
        }

        if (!$relatedResult) {
            throw new Exception("Failed to process related record for '{$relatedFieldName}'.");
        }

        $bindings = [];
        foreach ($modelRelatedFromFields as $i => $fromField) {
            $toField = $modelRelatedToFields[$i];
            if (!isset($relatedResult[$toField])) {
                throw new Exception("The field '{$toField}' is missing in the related data for '{$relatedFieldName}'.");
            }
            $bindings[$fromField] = $relatedResult[$toField];
        }
        return $bindings;
    }

    public static function populateIncludedRelations(
        array  $records,
        array  $includes,
        array  $fields,
        array  $fieldsRelatedWithKeys,
        PDO    $pdo,
        string $dbType,
    ): array {
        $isSingle = !isset($records[0]) || !is_array($records[0]);
        if ($isSingle) {
            $records = [$records];
        }

        $virtualFields = ['_count', '_max', '_min', '_avg', '_sum'];
        foreach ($virtualFields as $virtualField) {
            if (!isset($includes[$virtualField])) {
                continue;
            }

            $aggregateOptions = $includes[$virtualField];
            if (isset($aggregateOptions['select'])) {
                foreach ($records as $idx => $record) {
                    $records[$idx][$virtualField] = [];

                    foreach ($aggregateOptions['select'] as $relationName => $enabled) {
                        if (!$enabled || !isset($fields[$relationName], $fieldsRelatedWithKeys[$relationName])) {
                            continue;
                        }

                        $count = self::countRelatedRecords(
                            $record,
                            $relationName,
                            $fields[$relationName],
                            $fieldsRelatedWithKeys[$relationName],
                            $pdo,
                            $dbType
                        );

                        $records[$idx][$virtualField][$relationName] = $count;
                    }
                }
            }
        }

        foreach ($includes as $relationName => $relationOpts) {
            if (in_array($relationName, $virtualFields)) {
                continue;
            }

            if ($relationOpts === false) {
                continue;
            }
            if (!isset($fields[$relationName], $fieldsRelatedWithKeys[$relationName])) {
                continue;
            }

            $relatedField     = $fields[$relationName];
            $relatedKeys      = $fieldsRelatedWithKeys[$relationName];
            $relatedInstance  = self::makeRelatedInstance($relatedField['type'], $pdo);

            $instanceField = self::pickOppositeField(
                $relatedInstance->_fields,
                $relatedField['relationName'],
                $relatedField['isList']
            );

            if ($relatedField['isList'] && !$instanceField['isList']) {
                $childFk  = $instanceField['relationFromFields'][0] ?? null;
                $parentPk = $instanceField['relationToFields'][0]   ?? null;
                if ($childFk === null || $parentPk === null) {
                    goto PER_RECORD;
                }

                $parentIds = array_values(
                    array_unique(
                        array_filter(
                            array_column($records, $parentPk),
                            static fn($v) => $v !== null
                        )
                    )
                );
                if (!$parentIds) {
                    foreach ($records as &$rec) {
                        $rec[$relationName] = [];
                    }
                    unset($rec);
                    continue;
                }

                [$base] = self::buildQueryOptions(
                    [],
                    $relationOpts,
                    $relatedField,
                    $relatedKeys,
                    $instanceField
                );

                $groups = self::loadOneToManyBatch($relatedInstance, $childFk, $parentIds, $base);

                foreach ($records as &$rec) {
                    $rec[$relationName] = $groups[$rec[$parentPk]] ?? [];
                }
                unset($rec);
                continue;
            }

            PER_RECORD:
            foreach ($records as $idx => $singleRecord) {
                [$baseQuery, $where] = self::buildQueryOptions(
                    $singleRecord,
                    $relationOpts,
                    $relatedField,
                    $relatedKeys,
                    $instanceField
                );

                if ($relatedField['isList'] && $instanceField['isList']) {
                    $result = ($relatedField['type'] === $instanceField['type'])
                        ? self::loadExplicitMany($relatedInstance, $relatedField, $instanceField, $singleRecord, $baseQuery, $fields)
                        : self::loadImplicitMany($relatedInstance, $relatedField, $instanceField, $singleRecord, $baseQuery, $where, $dbType, $pdo);
                } elseif ($relatedField['isList']) {
                    $result = self::loadOneToMany($relatedInstance, $baseQuery);
                } else {
                    $result = self::loadOneToOne($relatedInstance, $baseQuery);
                }

                $records[$idx][$relationName] = $result;
            }
        }

        return $isSingle ? $records[0] : $records;
    }

    private static function countRelatedRecords(
        array $record,
        string $relationName,
        array $relatedField,
        array $relatedKeys,
        PDO $pdo,
        string $dbType
    ): int {
        $relatedInstance = self::makeRelatedInstance($relatedField['type'], $pdo);

        if (!empty($relatedKeys['relationFromFields']) && !empty($relatedKeys['relationToFields'])) {
            $conditions = [];
            foreach ($relatedKeys['relationFromFields'] as $i => $fromField) {
                $toField = $relatedKeys['relationToFields'][$i];

                if (isset($relatedInstance->_fields[$fromField])) {
                    $conditions[$fromField] = $record[$toField] ?? null;
                } else {
                    $conditions[$toField] = $record[$fromField] ?? null;
                }
            }

            if (empty(array_filter($conditions))) {
                return 0;
            }

            $whereClause = [];
            $bindings = [];
            $counter = 0;

            foreach ($conditions as $field => $value) {
                $placeholder = ':count_' . $counter++;
                $quotedField = self::quoteColumnName($dbType, $field);
                $whereClause[] = "$quotedField = $placeholder";
                $bindings[$placeholder] = $value;
            }

            $tableName = self::quoteColumnName($dbType, $relatedInstance->_tableName);
            $sql = "SELECT COUNT(*) FROM $tableName WHERE " . implode(' AND ', $whereClause);

            $stmt = $pdo->prepare($sql);
            foreach ($bindings as $key => $value) {
                $stmt->bindValue($key, $value);
            }
            $stmt->execute();

            return (int) $stmt->fetchColumn();
        }

        if (empty($relatedKeys['relationFromFields']) && empty($relatedKeys['relationToFields'])) {
            $implicitModelInfo = self::compareStringsAlphabetically($relatedField['type'], get_class($relatedInstance));
            $searchColumn = ($relatedField['type'] === $implicitModelInfo['A']) ? 'B' : 'A';

            $idField = null;
            foreach ($record as $key => $value) {
                if ($key === 'id' || str_ends_with($key, 'Id')) {
                    $idField = $key;
                    break;
                }
            }

            if (!$idField || !isset($record[$idField])) {
                return 0;
            }

            $tableName = self::quoteColumnName($dbType, $implicitModelInfo['Name']);
            $searchColumnQuoted = self::quoteColumnName($dbType, $searchColumn);

            $sql = "SELECT COUNT(*) FROM $tableName WHERE $searchColumnQuoted = :id";
            $stmt = $pdo->prepare($sql);
            $stmt->execute(['id' => $record[$idField]]);

            return (int) $stmt->fetchColumn();
        }

        return 0;
    }

    private static function pickOppositeField(array $allFields, string $relationName, bool $isListOnCaller): array
    {
        $candidates = array_values(array_filter(
            $allFields,
            static fn($f) => ($f['relationName'] ?? null) === $relationName
        ));

        if (count($candidates) === 1) {
            return $candidates[0];
        }

        foreach ($candidates as $f) {
            if ($f['isList'] !== $isListOnCaller) {
                return $f;
            }
        }

        return $candidates[0];
    }

    private static function loadOneToManyBatch(
        object $relatedInstance,
        string $childFk,
        array  $parentIds,
        array  $baseQuery,
    ): array {
        if (!isset($baseQuery['where'][$childFk])) {
            $baseQuery['where'][$childFk] = ['in' => $parentIds];
        }

        if (isset($baseQuery['select']) && $baseQuery['select'] !== []) {
            $baseQuery['select'][$childFk] = true;
        }

        $rows = $relatedInstance->findMany($baseQuery);

        $grouped = [];
        foreach ($rows as $row) {
            $key = $row->{$childFk};
            $grouped[$key][] = $row;
        }

        return $grouped;
    }

    private static function loadOneToOne(object $relatedInstance, array $query): array|object|null
    {
        return $relatedInstance->findUnique($query);
    }

    private static function loadOneToMany(object $relatedInstance, array $query): array
    {
        return $relatedInstance->findMany($query);
    }

    private static function loadExplicitMany(
        object $relatedInstance,
        array  $relatedField,
        array  $relatedInstanceField,
        array  $singleRecord,
        array  $queryOptions,
        array  $parentFields,
    ): array {
        if (isset($queryOptions['where']) && $queryOptions['where'] === []) {
            unset($queryOptions['where']);
        }

        if ($queryOptions === []) {
            $opposites = array_values(array_filter(
                $parentFields,
                fn($f) => ($f['relationName'] ?? null) === $relatedInstanceField['relationName'] &&
                    $f['name']               !== $relatedInstanceField['name']
            ));

            if ($opposites && isset($opposites[0]['relationFromFields'][0])) {
                $src = $opposites[0]['relationFromFields'][0];
                $dst = $opposites[0]['relationToFields'][0];
                $queryOptions['where'][$src] = $singleRecord[$dst] ?? null;
            }
        }

        return $relatedInstance->findMany($queryOptions);
    }

    private static function loadImplicitMany(
        object $relatedInstance,
        array  $relatedField,
        array  $relatedInstanceField,
        array  $singleRecord,
        array  $baseQuery,
        array  $whereConditions,
        string $dbType,
        PDO    $pdo,
    ): array {
        $info         = PPHPUtility::compareStringsAlphabetically($relatedField['type'], $relatedInstanceField['type']);
        $searchColumn = ($relatedField['type'] === $info['A']) ? 'B' : 'A';
        $returnColumn = $searchColumn === 'A' ? 'B' : 'A';
        $idField      = self::detectIdField($singleRecord, $relatedField, $relatedInstance);
        $idValue      = $singleRecord[$idField] ?? null;

        if ($idValue === null) {
            return [];
        }

        $table   = PPHPUtility::quoteColumnName($dbType, $info['Name']);
        $search  = PPHPUtility::quoteColumnName($dbType, $searchColumn);
        $return  = PPHPUtility::quoteColumnName($dbType, $returnColumn);
        $sql     = "SELECT {$return} FROM {$table} WHERE {$search} = :id";
        $stmt    = $pdo->prepare($sql);
        $stmt->execute(['id' => $idValue]);
        $ids     = $stmt->fetchAll(PDO::FETCH_COLUMN);

        if (!$ids) {
            return [];
        }

        $baseQuery['where'] = array_merge($whereConditions, ['id' => ['in' => $ids]]);
        return $relatedInstance->findMany($baseQuery);
    }

    private static function makeRelatedInstance(string $model, PDO $pdo): object
    {
        $fqcn = "Lib\\Prisma\\Classes\\{$model}";
        if (!class_exists($fqcn)) {
            throw new Exception("Class {$fqcn} does not exist.");
        }
        return (new ReflectionClass($fqcn))->newInstance($pdo);
    }

    private static function buildQueryOptions(
        array $singleRecord,
        mixed $relationOpts,
        array $relatedField,
        array $relatedFieldKeys,
        array $relatedInstanceField,
    ): array {
        if ($relationOpts === true) {
            $relationOpts = [];
        } elseif (!is_array($relationOpts)) {
            throw new Exception('include relation options must be array|true');
        }

        $where = [];
        foreach ($relatedFieldKeys['relationFromFields'] as $i => $fromField) {
            $toField = $relatedFieldKeys['relationToFields'][$i];

            if (!isset($singleRecord[$fromField]) && !isset($singleRecord[$toField])) {
                continue;
            }

            if (empty($relatedInstanceField['relationFromFields']) && empty($relatedInstanceField['relationToFields'])) {
                $where[$toField] = $singleRecord[$fromField] ?? $singleRecord[$toField] ?? null;
            } elseif ($relatedInstanceField['isList']) {
                $where[$toField] = $singleRecord[$fromField];
            } else {
                $where[$fromField] = $singleRecord[$toField];
            }
        }

        if (isset($relationOpts['where'])) {
            $where = array_merge($where, $relationOpts['where']);
        }

        $query = ['where' => $where];
        foreach (['select', 'include', 'omit'] as $clause) {
            if (!isset($relationOpts[$clause])) {
                continue;
            }
            $query[$clause] = self::normaliseClause($relationOpts[$clause]);
        }

        return [$query, $where];
    }

    private static function normaliseClause(array $raw): array
    {
        $out = [];
        foreach ($raw as $k => $v) {
            if (is_array($v)) {
                $out[$k] = $v;
            } elseif ((bool)$v === true) {
                $out[is_numeric($k) ? $v : $k] = true;
            }
        }
        return $out;
    }

    private static function detectIdField(array $singleRecord, array $relatedField, object $relatedInstance): string
    {
        foreach ($relatedField['relationFromFields'] as $from) {
            if (isset($singleRecord[$from])) {
                return $from;
            }
        }
        foreach ($relatedInstance->_fields as $f) {
            if ($f['isId']) {
                return $f['name'];
            }
        }
        throw new Exception('Unable to determine ID field for implicit many‑to‑many lookup.');
    }

    public static function sqlOperator(string $op): string
    {
        return match ($op) {
            'equals', '='  => '=',
            'gt'           => '>',
            'gte'          => '>=',
            'lt'           => '<',
            'lte'          => '<=',
            'not'          => '<>',
            'in'           => 'IN',
            'notIn'        => 'NOT IN',
            'between'      => 'BETWEEN',
            default        => throw new Exception("Unsupported operator '$op' in HAVING.")
        };
    }

    public static function buildHavingClause(
        array  $having,
        array  $aggMap,
        string $dbType,
        array  &$bindings
    ): string {
        if ($having === []) {
            return '';
        }

        $useAlias = $dbType !== 'pgsql';
        $clauses  = [];

        foreach ($having as $aggKey => $fields) {
            if (!isset($aggMap[$aggKey])) {
                throw new Exception("Unknown aggregate '$aggKey' in 'having'.");
            }
            $sqlFunc = $aggMap[$aggKey];

            foreach ($fields as $field => $comparators) {
                $alias = strtolower(substr($aggKey, 1)) . '_' . $field;
                $qf    = self::quoteColumnName($dbType, $field);
                $expr  = $useAlias ? $alias : "$sqlFunc($qf)";

                foreach ($comparators as $op => $value) {
                    $sqlOp = self::sqlOperator($op);

                    if ($sqlOp === 'BETWEEN') {
                        if (!is_array($value) || count($value) !== 2) {
                            throw new Exception("Operator 'between' expects exactly two values for '$alias'.");
                        }
                        $p1 = ':h' . count($bindings) . 'a';
                        $p2 = ':h' . count($bindings) . 'b';
                        $bindings[$p1] = $value[0];
                        $bindings[$p2] = $value[1];
                        $clauses[] = "$expr BETWEEN $p1 AND $p2";
                    } elseif (in_array($sqlOp, ['IN', 'NOT IN'], true)) {
                        if (!is_array($value) || $value === []) {
                            throw new Exception("Operator '$op' expects a non-empty array for '$alias'.");
                        }
                        $phs = [];
                        foreach ($value as $v) {
                            $p = ':h' . count($bindings);
                            $bindings[$p] = $v;
                            $phs[] = $p;
                        }
                        $clauses[] = "$expr $sqlOp (" . implode(', ', $phs) . ')';
                    } else {
                        $p = ':h' . count($bindings);
                        $bindings[$p] = $value;
                        $clauses[] = "$expr $sqlOp $p";
                    }
                }
            }
        }

        return $clauses ? ' HAVING ' . implode(' AND ', $clauses) : '';
    }

    public static function normalizeRowTypes(array $row, array $fieldsByName): array
    {
        foreach ($fieldsByName as $name => $meta) {
            if (!array_key_exists($name, $row)) {
                continue;
            }
            if (($meta['kind'] ?? null) !== 'scalar') {
                continue;
            }

            $type = $meta['type'] ?? null;
            $row[$name] = self::normalizeValueByType($row[$name], $type);
        }
        return $row;
    }

    public static function normalizeListTypes(array $rows, array $fieldsByName): array
    {
        foreach ($rows as $i => $row) {
            if (is_array($row)) {
                $rows[$i] = self::normalizeRowTypes($row, $fieldsByName);
            } elseif (is_object($row)) {
                $rows[$i] = (object) self::normalizeRowTypes((array) $row, $fieldsByName);
            }
        }
        return $rows;
    }

    private static function normalizeValueByType(mixed $value, ?string $type): mixed
    {
        if ($value === null) return null;

        switch ($type) {
            case 'Boolean':
                return self::toBool($value);
            case 'Int':
                return (int) $value;
            case 'BigInt':
                return (string) $value;
            case 'Decimal':
                return (string) $value;
            case 'DateTime':
                return Validator::dateTime($value);
            default:
                return $value;
        }
    }

    public static function toBool(mixed $v): bool
    {
        $b = Validator::boolean($v);
        if ($b !== null) return $b;

        if (is_numeric($v)) return ((int) $v) === 1;

        if (is_string($v)) {
            $s = strtolower(trim($v));
            if (in_array($s, ['t', 'y', 'yes'], true))  return true;
            if (in_array($s, ['f', 'n', 'no'], true))   return false;
        }

        return (bool) $v;
    }
}
