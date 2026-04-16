<?php

namespace Lib\Prisma\Classes;

use PP\Validator;
use Exception;
use InvalidArgumentException;
use PDO;
use PDOStatement;
use UnitEnum;

enum ArrayType: string
{
    case Associative = 'associative';
    case Indexed = 'indexed';
    case Value = 'value';
}

final class PPHPUtility
{
    /** @var array<string, array<string, array<string, mixed>>> */
    private static array $fieldMapCache = [];

    /** @var array<string, array<string, bool>> */
    private static array $fieldNameSetCache = [];

    /** @var array<int, array<string, object>> */
    private static array $relatedInstanceCache = [];

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
        $virtualFields = ['_count', '_max', '_min', '_avg', '_sum'];
        $fieldMap = self::fieldMap($fields, $modelName);
        $fieldNameSet = self::fieldNameSet($fields, $modelName);
        $relationNameSet = array_fill_keys($relationName, true);

        if (isset($select) && is_array($select)) {
            foreach ($select as $key => $value) {
                if ($key === $timestamp) continue;

                if (is_string($key) && in_array($key, $virtualFields, true)) {
                    if (is_array($value) && isset($value['select']) && is_array($value['select'])) {
                        $relatedEntityFields[$key] = $value;
                    } else {
                        $relatedEntityFields[$key] = $value;
                    }
                    continue;
                }

                if (is_numeric($key) && is_string($value)) {
                    if (isset($fieldNameSet[$value]))
                        throw new Exception("The '$value' is indexed, waiting example: ['$value' => true]");
                }

                if (isset($value) && empty($value) || !is_bool($value)) {
                    if (is_string($key) && !isset($fieldNameSet[$key])) {
                        throw new Exception("The field '$key' does not exist in the $modelName model.");
                    }

                    if (is_string($key) && isset($fieldNameSet[$key])) {
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
                        $fieldMeta = $fieldMap[$fieldName] ?? null;

                        if (!isset($fieldNameSet[$fieldName])) {
                            $availableFields = implode(', ', array_keys($fields));
                            throw new Exception("The field '$fieldName' does not exist in the $modelName model. Available fields are: $availableFields");
                        }

                        if (
                            isset($relationNameSet[$fieldName]) ||
                            (($fieldMeta['type'] ?? null) !== null && isset($relationNameSet[$fieldMeta['type']]))
                        ) {
                            $relatedEntityFields[$fieldName] = [$fieldName];
                            continue;
                        }

                        $isObject = false;
                        if (($fieldMeta['kind'] ?? null) === 'object') {
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
        $fieldMap = self::fieldMap($fields, $modelName);
        $fieldNameSet = self::fieldNameSet($fields, $modelName);

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
                if (self::fieldExists($key, $fields, $modelName))
                    throw new Exception("The '$value' is indexed, waiting example: ['$value' => true]");
            }

            if (isset($value) && empty($value) || !is_bool($value)) {
                if (is_string($key) && !self::fieldExists($key, $fields, $modelName)) {
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

                    $fieldMeta = $fieldMap[$key] ?? null;
                    $isRelatedModel = ($fieldMeta['kind'] ?? null) === 'object';
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
                if (!isset($fieldNameSet[$fieldName])) {
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

    private static function fieldExists(string|int $key, array $fields, string $modelName = ''): bool
    {
        $fieldNameSet = self::fieldNameSet($fields, $modelName);

        return isset($fieldNameSet[(string) $key]);
    }

    private static function fieldMap(array $fields, string $modelName = ''): array
    {
        $cacheKey = self::fieldCacheKey($fields, $modelName);
        if (isset(self::$fieldMapCache[$cacheKey])) {
            return self::$fieldMapCache[$cacheKey];
        }

        $fieldMap = [];
        foreach ($fields as $key => $field) {
            if (!is_array($field)) {
                continue;
            }

            $fieldName = is_string($field['name'] ?? null)
                ? $field['name']
                : (is_string($key) ? $key : null);

            if ($fieldName === null || $fieldName === '') {
                continue;
            }

            $fieldMap[$fieldName] = $field;
        }

        self::$fieldMapCache[$cacheKey] = $fieldMap;

        return $fieldMap;
    }

    private static function fieldNameSet(array $fields, string $modelName = ''): array
    {
        $cacheKey = self::fieldCacheKey($fields, $modelName);
        if (isset(self::$fieldNameSetCache[$cacheKey])) {
            return self::$fieldNameSetCache[$cacheKey];
        }

        self::$fieldNameSetCache[$cacheKey] = array_fill_keys(
            array_keys(self::fieldMap($fields, $modelName)),
            true
        );

        return self::$fieldNameSetCache[$cacheKey];
    }

    private static function fieldCacheKey(array $fields, string $modelName = ''): string
    {
        if ($modelName !== '') {
            return $modelName;
        }

        $fieldNames = [];
        foreach ($fields as $key => $field) {
            if (is_array($field) && is_string($field['name'] ?? null)) {
                $fieldNames[] = $field['name'];
                continue;
            }

            if (is_string($key)) {
                $fieldNames[] = $key;
            }
        }

        sort($fieldNames);

        return implode('|', $fieldNames);
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
        $fieldNameSet = self::fieldNameSet($fields, $modelName);

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

                if (is_array($value) && isset($fieldNameSet[$key])) {
                    $includes[$key] = $value;
                } elseif (is_bool($value)) {
                    $includes[$key] = $value;
                } elseif (!is_array($value)) {
                    throw new Exception("Invalid include format for '$key'. Expecting an array or boolean.");
                }

                if (!isset($fieldNameSet[$key])) {
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
    public static function processConditions(array $conditions, &$sqlConditions, &$bindings, $dbType, $tableName, $prefix = '', $level = 0, ?array $fieldContext = null)
    {
        $fieldContext ??= self::resolveConditionFieldContext($tableName);

        foreach ($conditions as $key => $value) {
            if (in_array($key, ['AND', 'OR', 'NOT'])) {
                $groupedConditions = [];
                if ($key === 'NOT') {
                    self::processNotCondition($value, $groupedConditions, $bindings, $dbType, $tableName, $prefix . $key . '_', $level, $fieldContext);
                    if (!empty($groupedConditions)) {
                        $conditionGroup = '(' . implode(" $key ", $groupedConditions) . ')';
                        $conditionGroup = 'NOT ' . $conditionGroup;
                        $sqlConditions[] = $conditionGroup;
                    }
                } else {
                    foreach ($value as $conditionKey => $subCondition) {
                        if (is_numeric($conditionKey)) {
                            self::processConditions($subCondition, $groupedConditions, $bindings, $dbType, $tableName, $prefix . $key . $conditionKey . '_', $level + 1, $fieldContext);
                        } else {
                            self::processSingleCondition($conditionKey, $subCondition, $groupedConditions, $bindings, $dbType, $tableName, $prefix . $key . $conditionKey . '_', $level + 1, $fieldContext);
                        }
                    }
                    if (!empty($groupedConditions)) {
                        $conditionGroup = '(' . implode(" $key ", $groupedConditions) . ')';
                        $sqlConditions[] = $conditionGroup;
                    }
                }
            } else {
                self::processSingleCondition($key, $value, $sqlConditions, $bindings, $dbType, $tableName, $prefix, $level, $fieldContext);
            }
        }
    }

    private static function conditionOperators(): array
    {
        return [
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
            'mode',
            'increment',
            'decrement',
            'multiply',
            'divide',
        ];
    }

    private static function isOperatorArray(array $arr)
    {
        $operators = self::conditionOperators();
        foreach ($arr as $key => $value) {
            if (!in_array($key, $operators, true)) {
                return false;
            }
        }
        return true;
    }

    private static function hasOperatorKey(array $arr): bool
    {
        $operators = self::conditionOperators();

        foreach (array_keys($arr) as $key) {
            if (in_array($key, $operators, true)) {
                return true;
            }
        }

        return false;
    }

    private static function unsupportedOperatorKeys(array $arr): array
    {
        $operators = self::conditionOperators();

        return array_values(array_filter(
            array_keys($arr),
            static fn(mixed $key): bool => is_string($key) && !in_array($key, $operators, true)
        ));
    }

    private static function processSingleCondition($key, $value, &$sqlConditions, &$bindings, $dbType, $tableName, $prefix, $level, ?array $fieldContext = null)
    {
        if (is_array($value) && !self::isOperatorArray($value)) {
            if (self::hasOperatorKey($value)) {
                $unsupportedKeys = self::unsupportedOperatorKeys($value);
                if ($unsupportedKeys !== []) {
                    throw new Exception(
                        "Unsupported condition(s) for '$key': " . implode(', ', $unsupportedKeys) . '.'
                    );
                }
            }

            foreach ($value as $nestedKey => $nestedValue) {
                self::processSingleCondition(
                    $nestedKey,
                    $nestedValue,
                    $sqlConditions,
                    $bindings,
                    $dbType,
                    $tableName,
                    $prefix . $key . '_',
                    $level + 1,
                    $fieldContext
                );
            }
            return;
        }

        $fieldQuoted = self::quoteColumnName($dbType, $key);
        $qualifiedField = $tableName . '.' . $fieldQuoted;
        $fieldMeta = is_array($fieldContext[$key] ?? null) ? $fieldContext[$key] : null;

        if (is_array($value)) {
            $queryMode = self::resolveStringQueryMode($key, $value, $fieldMeta, $dbType);

            if (array_key_exists('mode', $value)) {
                unset($value['mode']);

                if ($value === []) {
                    throw new Exception("Query mode on '$key' requires at least one string filter operator.");
                }
            }

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
                $fieldReferenceExpression = self::resolveFieldReferenceExpression(
                    $val,
                    $key,
                    $fieldMeta,
                    $fieldContext,
                    $dbType,
                    $tableName
                );

                switch ($condition) {
                    case 'contains':
                    case 'startsWith':
                    case 'endsWith':
                        if ($fieldReferenceExpression !== null) {
                            throw new Exception("Field references are not supported for '$condition' on '$key'.");
                        }

                        if ($val === null) {
                            $sqlConditions[] = "$qualifiedField IS NULL";
                        } elseif ($val === '') {
                            $sqlConditions[] = "$qualifiedField = ''";
                        } else {
                            $validatedValue = Validator::string($val, false);
                            $likeOperator = self::usesInsensitiveStringMode($queryMode, $dbType) ? 'ILIKE' : 'LIKE';
                            if ($condition === 'startsWith') $validatedValue .= '%';
                            if ($condition === 'endsWith') $validatedValue = '%' . $validatedValue;
                            if ($condition === 'contains') $validatedValue = '%' . $validatedValue . '%';
                            $sqlConditions[] = "$qualifiedField $likeOperator $bindingKey";
                            $bindings[$bindingKey] = $validatedValue;
                        }
                        break;
                    case 'equals':
                        if ($fieldReferenceExpression !== null) {
                            $leftExpression = self::modeAwareStringExpression($qualifiedField, $queryMode, $dbType);
                            $rightExpression = self::modeAwareStringExpression($fieldReferenceExpression, $queryMode, $dbType);
                            $sqlConditions[] = "$leftExpression = $rightExpression";
                        } elseif ($val === null) {
                            $sqlConditions[] = "$qualifiedField IS NULL";
                        } elseif ($val === '' && !self::usesInsensitiveStringMode($queryMode, $dbType)) {
                            $sqlConditions[] = "$qualifiedField = ''";
                        } else {
                            $validatedValue = self::normalizeEqualityConditionValue($val, $fieldMeta);
                            $fieldExpression = self::modeAwareStringExpression($qualifiedField, $queryMode, $dbType);
                            $bindingExpression = self::modeAwareStringExpression(
                                self::formatConditionBindingExpression($bindingKey, $fieldMeta, $dbType),
                                $queryMode,
                                $dbType
                            );
                            $sqlConditions[] = "$fieldExpression = $bindingExpression";
                            $bindings[$bindingKey] = $validatedValue;
                        }
                        break;
                    case 'not':
                        if ($fieldReferenceExpression !== null) {
                            $leftExpression = self::modeAwareStringExpression($qualifiedField, $queryMode, $dbType);
                            $rightExpression = self::modeAwareStringExpression($fieldReferenceExpression, $queryMode, $dbType);
                            $sqlConditions[] = "$leftExpression != $rightExpression";
                        } elseif ($val === null) {
                            $sqlConditions[] = "$qualifiedField IS NOT NULL";
                        } elseif ($val === '' && !self::usesInsensitiveStringMode($queryMode, $dbType)) {
                            $sqlConditions[] = "$qualifiedField != ''";
                        } else {
                            $validatedValue = self::normalizeEqualityConditionValue($val, $fieldMeta);
                            $fieldExpression = self::modeAwareStringExpression($qualifiedField, $queryMode, $dbType);
                            $bindingExpression = self::modeAwareStringExpression(
                                self::formatConditionBindingExpression($bindingKey, $fieldMeta, $dbType),
                                $queryMode,
                                $dbType
                            );
                            $sqlConditions[] = "$fieldExpression != $bindingExpression";
                            $bindings[$bindingKey] = $validatedValue;
                        }
                        break;
                    case 'gt':
                    case 'gte':
                    case 'lt':
                    case 'lte':
                        $operator = $condition === 'gt' ? '>' : ($condition === 'gte' ? '>=' : ($condition === 'lt' ? '<' : '<='));

                        if ($fieldReferenceExpression !== null) {
                            $leftExpression = self::modeAwareStringExpression($qualifiedField, $queryMode, $dbType);
                            $rightExpression = self::modeAwareStringExpression($fieldReferenceExpression, $queryMode, $dbType);
                            $sqlConditions[] = "$leftExpression $operator $rightExpression";
                        } else {
                            $validatedValue = self::normalizeRangeConditionValue($val, $fieldMeta);
                            $fieldExpression = self::modeAwareStringExpression($qualifiedField, $queryMode, $dbType);
                            $bindingExpression = self::modeAwareStringExpression($bindingKey, $queryMode, $dbType);
                            $sqlConditions[] = "$fieldExpression $operator $bindingExpression";
                            $bindings[$bindingKey] = $validatedValue;
                        }

                        break;
                    case 'in':
                    case 'notIn':
                        $fieldExpression = self::modeAwareStringExpression($qualifiedField, $queryMode, $dbType);
                        $inPlaceholders = [];
                        foreach ($val as $i => $inVal) {
                            $inKey = $bindingKey . "_" . $i;
                            $validatedValue = self::normalizeEqualityConditionValue($inVal, $fieldMeta);
                            $inPlaceholders[] = self::modeAwareStringExpression(
                                self::formatConditionBindingExpression($inKey, $fieldMeta, $dbType),
                                $queryMode,
                                $dbType
                            );
                            $bindings[$inKey] = $validatedValue;
                        }
                        $inClause = implode(', ', $inPlaceholders);
                        $sqlConditions[] = "$fieldExpression " . ($condition === 'notIn' ? 'NOT IN' : 'IN') . " ($inClause)";
                        break;
                    default:
                        // Handle other conditions or log an error/warning for unsupported conditions
                        throw new Exception("Unsupported condition: $condition");
                        break;
                }
            }
        } else {
            $fieldReferenceExpression = self::resolveFieldReferenceExpression(
                $value,
                $key,
                $fieldMeta,
                $fieldContext,
                $dbType,
                $tableName
            );

            if ($fieldReferenceExpression !== null) {
                $sqlConditions[] = "$qualifiedField = $fieldReferenceExpression";
            } elseif ($value === null) {
                $sqlConditions[] = "$qualifiedField IS NULL";
            } elseif ($value === '') {
                $sqlConditions[] = "$qualifiedField = ''";
            } else {
                if ($value instanceof UnitEnum) {
                    $value = $value->value;
                }

                $bindingKey = ":" . $prefix . $key . $level;
                $validatedValue = self::normalizeEqualityConditionValue($value, $fieldMeta);
                $bindingExpression = self::formatConditionBindingExpression($bindingKey, $fieldMeta, $dbType);
                $sqlConditions[] = "$qualifiedField = $bindingExpression";
                $bindings[$bindingKey] = $validatedValue;
            }
        }
    }

    private static function resolveStringQueryMode(string $fieldName, array $filter, ?array $fieldMeta, string $dbType): ?string
    {
        if (!array_key_exists('mode', $filter)) {
            return null;
        }

        $mode = $filter['mode'];
        if (!is_string($mode) || trim($mode) === '') {
            throw new Exception("Query mode for '$fieldName' must be a non-empty string.");
        }

        $normalizedMode = strtolower(trim($mode));
        if (!in_array($normalizedMode, ['default', 'insensitive'], true)) {
            throw new Exception(
                "Invalid query mode '$mode' for '$fieldName'. Supported modes: default, insensitive."
            );
        }

        if ($dbType !== 'pgsql') {
            throw new Exception(
                "Query mode '$normalizedMode' for '$fieldName' is only supported for PostgreSQL string filters."
            );
        }

        if ($fieldMeta !== null && !self::isStringFieldMeta($fieldMeta)) {
            throw new Exception("Query mode '$normalizedMode' for '$fieldName' is only supported for String fields.");
        }

        return $normalizedMode;
    }

    private static function isStringFieldMeta(?array $fieldMeta): bool
    {
        return ($fieldMeta['kind'] ?? null) === 'scalar' && ($fieldMeta['type'] ?? null) === 'String';
    }

    private static function usesInsensitiveStringMode(?string $queryMode, string $dbType): bool
    {
        return $dbType === 'pgsql' && $queryMode === 'insensitive';
    }

    private static function modeAwareStringExpression(string $expression, ?string $queryMode, string $dbType): string
    {
        if (!self::usesInsensitiveStringMode($queryMode, $dbType)) {
            return $expression;
        }

        return "LOWER($expression)";
    }

    private static function resolveFieldReferenceExpression(
        mixed $value,
        string $currentFieldName,
        ?array $currentFieldMeta,
        ?array $fieldContext,
        string $dbType,
        string $tableName,
    ): ?string {
        if (!$value instanceof ModelFieldReference) {
            return null;
        }

        $referencedFieldName = $value->fieldName();
        $referencedFieldMeta = is_array($fieldContext[$referencedFieldName] ?? null)
            ? $fieldContext[$referencedFieldName]
            : $value->fieldMeta();

        if ($referencedFieldMeta === null) {
            throw new Exception("Field reference '$referencedFieldName' does not exist in the current model context.");
        }

        if (!in_array($referencedFieldMeta['kind'] ?? null, ['scalar', 'enum'], true)) {
            throw new Exception("Field reference '$referencedFieldName' must target a scalar or enum field.");
        }

        if (!self::areComparableFieldReferenceTypes($currentFieldMeta, $referencedFieldMeta)) {
            $currentType = (string) ($currentFieldMeta['type'] ?? 'unknown');
            $referencedType = (string) ($referencedFieldMeta['type'] ?? 'unknown');

            throw new Exception(
                "Field reference '$referencedFieldName' is not comparable with '$currentFieldName' ({$currentType} vs {$referencedType})."
            );
        }

        return $tableName . '.' . self::quoteColumnName($dbType, $referencedFieldName);
    }

    private static function areComparableFieldReferenceTypes(?array $currentFieldMeta, ?array $referencedFieldMeta): bool
    {
        if ($currentFieldMeta === null || $referencedFieldMeta === null) {
            return true;
        }

        $currentKind = $currentFieldMeta['kind'] ?? null;
        $referencedKind = $referencedFieldMeta['kind'] ?? null;

        if ($currentKind !== $referencedKind) {
            return false;
        }

        if (!in_array($currentKind, ['scalar', 'enum'], true)) {
            return false;
        }

        return ($currentFieldMeta['type'] ?? null) === ($referencedFieldMeta['type'] ?? null);
    }

    private static function processNotCondition($conditions, &$sqlConditions, &$bindings, $dbType, $tableName, $prefix, $level = 0, ?array $fieldContext = null)
    {
        foreach ($conditions as $key => $value) {
            self::processSingleCondition($key, $value, $sqlConditions, $bindings, $dbType, $tableName, $prefix . 'NOT_', $level, $fieldContext);
        }
    }

    private static function resolveConditionFieldContext(string $tableName): ?array
    {
        $normalizedTableName = self::normalizeSqlIdentifier($tableName);
        if ($normalizedTableName === '') {
            return null;
        }

        $trace = debug_backtrace(DEBUG_BACKTRACE_PROVIDE_OBJECT | DEBUG_BACKTRACE_IGNORE_ARGS);
        foreach ($trace as $frame) {
            $model = $frame['object'] ?? null;
            if (!is_object($model)) {
                continue;
            }

            $className = get_class($model);
            if (!str_starts_with($className, __NAMESPACE__ . '\\') || $model instanceof self) {
                continue;
            }

            $modelTableName = self::normalizeSqlIdentifier((string) ($model->_tableName ?? ''));
            if ($modelTableName !== $normalizedTableName) {
                continue;
            }

            $fields = $model->_fields ?? null;
            if (is_array($fields)) {
                return $fields;
            }
        }

        return null;
    }

    private static function normalizeSqlIdentifier(string $identifier): string
    {
        $trimmed = trim($identifier);
        if ($trimmed === '') {
            return '';
        }

        $parts = explode('.', $trimmed);
        $lastPart = end($parts);

        return trim((string) $lastPart, " \t\n\r\0\x0B\"`[]");
    }

    private static function normalizeEqualityConditionValue(mixed $value, ?array $fieldMeta): mixed
    {
        $typedValue = self::tryNormalizeTypedConditionValue($value, $fieldMeta);
        if ($typedValue['handled']) {
            return $typedValue['value'];
        }

        return Validator::string($value, false);
    }

    private static function normalizeRangeConditionValue(mixed $value, ?array $fieldMeta): mixed
    {
        $typedValue = self::tryNormalizeTypedConditionValue($value, $fieldMeta);
        if ($typedValue['handled']) {
            return $typedValue['value'];
        }

        if (is_float($value)) {
            return Validator::float($value);
        }

        if (is_int($value)) {
            return Validator::int($value);
        }

        if (is_string($value) && strtotime($value) !== false) {
            return date('Y-m-d H:i:s', strtotime($value));
        }

        return Validator::string($value, false);
    }

    private static function tryNormalizeTypedConditionValue(mixed $value, ?array $fieldMeta): array
    {
        if ($fieldMeta === null) {
            return ['handled' => false, 'value' => null];
        }

        if ($value instanceof UnitEnum) {
            $value = $value->value;
        }

        $kind = $fieldMeta['kind'] ?? null;
        $type = $fieldMeta['type'] ?? null;

        if ($kind === 'enum') {
            return ['handled' => true, 'value' => Validator::string($value, false)];
        }

        if ($kind !== 'scalar') {
            return ['handled' => false, 'value' => null];
        }

        return match ($type) {
            'Boolean' => self::normalizeBooleanConditionValue($value),
            'Int' => self::normalizeIntegerConditionValue($value),
            'Float' => self::normalizeFloatConditionValue($value),
            'BigInt' => self::normalizeBigIntConditionValue($value),
            'Decimal' => ['handled' => is_scalar($value), 'value' => is_scalar($value) ? Validator::string($value, false) : null],
            'DateTime' => self::normalizeDateTimeConditionValue($value),
            'Bytes' => self::normalizeBytesConditionValue($value),
            default => ['handled' => false, 'value' => null],
        };
    }

    private static function normalizeBooleanConditionValue(mixed $value): array
    {
        $bool = Validator::boolean($value);

        if ($bool !== null) {
            return ['handled' => true, 'value' => $bool];
        }

        if (is_numeric($value)) {
            return ['handled' => true, 'value' => ((int) $value) === 1];
        }

        return ['handled' => false, 'value' => null];
    }

    private static function normalizeIntegerConditionValue(mixed $value): array
    {
        $intValue = Validator::int($value);

        return $intValue === null
            ? ['handled' => false, 'value' => null]
            : ['handled' => true, 'value' => $intValue];
    }

    private static function normalizeFloatConditionValue(mixed $value): array
    {
        $floatValue = Validator::float($value);

        return $floatValue === null
            ? ['handled' => false, 'value' => null]
            : ['handled' => true, 'value' => $floatValue];
    }

    private static function normalizeBigIntConditionValue(mixed $value): array
    {
        $bigIntValue = Validator::bigInt($value);

        return $bigIntValue === null
            ? ['handled' => false, 'value' => null]
            : ['handled' => true, 'value' => (string) $bigIntValue];
    }

    private static function normalizeDateTimeConditionValue(mixed $value): array
    {
        $dateTimeValue = Validator::dateTime($value);

        return $dateTimeValue === null
            ? ['handled' => false, 'value' => null]
            : ['handled' => true, 'value' => $dateTimeValue];
    }

    private static function normalizeBytesConditionValue(mixed $value): array
    {
        if (is_string($value)) {
            return ['handled' => true, 'value' => $value];
        }

        if (is_array($value) && array_is_list($value)) {
            $byteValues = array_map(
                static fn(mixed $byte): ?int => is_int($byte) && $byte >= 0 && $byte <= 255 ? $byte : null,
                $value
            );

            if (!in_array(null, $byteValues, true)) {
                return ['handled' => true, 'value' => pack('C*', ...$byteValues)];
            }
        }

        return ['handled' => false, 'value' => null];
    }

    private static function formatConditionBindingExpression(string $bindingKey, ?array $fieldMeta, string $dbType): string
    {
        if (($fieldMeta['kind'] ?? null) === 'scalar' && ($fieldMeta['type'] ?? null) === 'Bytes' && $dbType === 'sqlite') {
            return "CAST($bindingKey AS BLOB)";
        }

        return $bindingKey;
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

    public static function normalizeDistinctFields(mixed $distinct): array
    {
        if (is_string($distinct) && $distinct !== '') {
            return [$distinct];
        }

        if (!is_array($distinct)) {
            return [];
        }

        if (array_is_list($distinct)) {
            return array_values(array_filter(
                $distinct,
                static fn(mixed $field): bool => is_string($field) && $field !== ''
            ));
        }

        return array_values(array_filter(
            array_keys(array_filter($distinct, static fn(mixed $enabled): bool => $enabled === true)),
            static fn(mixed $field): bool => is_string($field) && $field !== ''
        ));
    }

    public static function applyDistinctRows(array $records, array $distinctFields): array
    {
        if ($distinctFields === []) {
            return $records;
        }

        $seen = [];
        $deduplicated = [];

        foreach ($records as $record) {
            $signature = implode("\x1F", array_map(
                static fn(string $field): string => serialize($record[$field] ?? null),
                $distinctFields
            ));

            if (isset($seen[$signature])) {
                continue;
            }

            $seen[$signature] = true;
            $deduplicated[] = $record;
        }

        return $deduplicated;
    }

    public static function applySkipTakeToRows(array $records, ?int $skip = null, ?int $take = null): array
    {
        $skip = $skip !== null ? max(0, $skip) : null;

        if ($take !== null && $take < 0) {
            $endExclusive = max(0, count($records) - ($skip ?? 0));
            $start = max(0, $endExclusive - abs($take));

            return array_values(array_slice($records, $start, max(0, $endExclusive - $start)));
        }

        if ($skip !== null && $skip > 0) {
            $records = array_slice($records, $skip);
        }

        if ($take !== null) {
            $records = array_slice($records, 0, max(0, $take));
        }

        return array_values($records);
    }

    public static function shouldApplyPaginationInPhp(array $criteria, array $distinctFields = []): bool
    {
        if ($distinctFields !== []) {
            return true;
        }

        if (isset($criteria['cursor']) && is_array($criteria['cursor']) && $criteria['cursor'] !== []) {
            return true;
        }

        return isset($criteria['take']) && intval($criteria['take']) < 0;
    }

    public static function stripPaginationForPhpProcessing(array $criteria, array $distinctFields = []): array
    {
        if (!self::shouldApplyPaginationInPhp($criteria, $distinctFields)) {
            return $criteria;
        }

        unset($criteria['skip'], $criteria['take']);

        return $criteria;
    }

    public static function applyCursorAndPaginationToRows(array $records, array $criteria): array
    {
        $skip = isset($criteria['skip']) ? intval($criteria['skip']) : null;
        $take = isset($criteria['take']) ? intval($criteria['take']) : null;
        $cursor = $criteria['cursor'] ?? null;

        if (is_array($cursor) && $cursor !== []) {
            $cursorIndex = self::findCursorIndex($records, $cursor);

            if ($cursorIndex === null) {
                return [];
            }

            if ($take !== null && $take < 0) {
                $records = array_slice($records, 0, $cursorIndex + 1);
            } else {
                $records = array_slice($records, $cursorIndex);
            }
        }

        return self::applySkipTakeToRows($records, $skip, $take);
    }

    public static function stripNonRecordWindowingForPhpProcessing(array $criteria): array
    {
        unset($criteria['cursor'], $criteria['skip'], $criteria['take']);

        return $criteria;
    }

    public static function computeCountResultFromRows(array $rows, array $select, array $fields, string $modelName): int|object
    {
        if ($select === []) {
            return count($rows);
        }

        self::checkFieldsExist($select, $fields, $modelName);

        $out = [];
        foreach (array_keys($select) as $fieldName) {
            $out[$fieldName] = count(array_filter(
                $rows,
                static fn(array $row): bool => array_key_exists($fieldName, $row) && $row[$fieldName] !== null
            ));
        }

        return (object) $out;
    }

    public static function castAggregateValue(string $op, ?string $fieldName, mixed $value, array $fields): mixed
    {
        if ($value === null) {
            return null;
        }

        if ($op === '_count') {
            return (int) $value;
        }

        $type = $fieldName !== null && isset($fields[$fieldName]['type'])
            ? $fields[$fieldName]['type']
            : null;

        return match ($op) {
            '_sum' => match ($type) {
                'Int' => (int) $value,
                'BigInt', 'Decimal' => (string) $value,
                default => is_numeric($value) ? +$value : $value,
            },
            '_avg' => match ($type) {
                'Decimal', 'BigInt' => (string) $value,
                default => is_numeric($value) ? (float) $value : $value,
            },
            '_min', '_max' => match ($type) {
                'Int' => (int) $value,
                'BigInt', 'Decimal' => (string) $value,
                'Boolean' => self::toBool($value),
                'DateTime' => Validator::dateTime($value),
                default => $value,
            },
            default => $value,
        };
    }

    public static function computeAggregateResultFromRows(array $rows, array $operation, array $fields, string $modelName): object
    {
        $aggregateFunctions = ['_avg', '_count', '_max', '_min', '_sum'];
        $hasAggregateRequest = false;
        $aggregateResult = [];

        foreach ($aggregateFunctions as $aggregateKey) {
            if (!array_key_exists($aggregateKey, $operation)) {
                continue;
            }

            $hasAggregateRequest = true;

            if ($aggregateKey === '_count' && $operation['_count'] === true) {
                $aggregateResult['_count']['_all'] = count($rows);
                continue;
            }

            $fieldsReq = $operation[$aggregateKey];
            if (!is_array($fieldsReq)) {
                throw new Exception("'$aggregateKey' must be an array (or 'true' for _count).");
            }

            foreach ($fieldsReq as $field => $enabled) {
                if (!$enabled) {
                    continue;
                }

                if ($aggregateKey === '_count' && $field === '_all') {
                    $aggregateResult['_count']['_all'] = count($rows);
                    continue;
                }

                if (!isset($fields[$field])) {
                    throw new Exception("Field '$field' does not exist in {$modelName}.");
                }

                $values = [];
                foreach ($rows as $row) {
                    if (!array_key_exists($field, $row) || $row[$field] === null) {
                        continue;
                    }

                    $values[] = $row[$field];
                }

                $rawValue = match ($aggregateKey) {
                    '_count' => count($values),
                    '_min' => $values === [] ? null : min($values),
                    '_max' => $values === [] ? null : max($values),
                    '_sum' => self::calculateAggregateValue('_sum', $values),
                    '_avg' => self::calculateAggregateValue('_avg', $values),
                    default => null,
                };

                $aggregateResult[$aggregateKey][$field] = self::castAggregateValue(
                    $aggregateKey,
                    $field,
                    $rawValue,
                    $fields
                );
            }
        }

        if (!$hasAggregateRequest) {
            throw new Exception('No valid aggregate function specified.');
        }

        foreach ($aggregateResult as $aggregateKey => $values) {
            if (is_array($values)) {
                $aggregateResult[$aggregateKey] = (object) $values;
            }
        }

        return (object) $aggregateResult;
    }

    private static function calculateAggregateValue(string $aggregateKey, array $values): mixed
    {
        if ($values === []) {
            return null;
        }

        $total = array_reduce(
            $values,
            static fn(int|float $carry, mixed $value): int|float => $carry + (is_numeric($value) ? +$value : 0),
            0
        );

        if ($aggregateKey === '_sum') {
            return $total;
        }

        if ($aggregateKey === '_avg') {
            return $total / count($values);
        }

        return null;
    }

    private static function findCursorIndex(array $records, array $cursor): ?int
    {
        foreach ($records as $index => $record) {
            $matches = true;

            foreach ($cursor as $field => $value) {
                if (!array_key_exists($field, $record) || $record[$field] != $value) {
                    $matches = false;
                    break;
                }
            }

            if ($matches) {
                return $index;
            }
        }

        return null;
    }

    public static function normalizeBulkMutationLimit(array $criteria): ?int
    {
        if (!array_key_exists('limit', $criteria)) {
            return null;
        }

        $limit = intval($criteria['limit']);

        if ($limit < 0) {
            throw new InvalidArgumentException("Input error. Provided limit ({$limit}) must be a positive integer.");
        }

        return $limit;
    }

    public static function buildLimitedMutationWhereClause(
        string $quotedTableName,
        array $primaryKeyFields,
        string $dbType,
        array $where,
        array &$bindings,
        int $limit,
        string $bindingPrefix = 'limit'
    ): string {
        $subqueryConditions = [];
        self::processConditions($where, $subqueryConditions, $bindings, $dbType, $quotedTableName);

        $quotedPrimaryKeyFields = array_map(
            static fn(string $field): string => self::quoteColumnName($dbType, $field),
            $primaryKeyFields
        );

        $subqueryFieldList = implode(', ', array_map(
            static fn(string $field): string => $quotedTableName . '.' . $field,
            $quotedPrimaryKeyFields
        ));

        $innerSql = "SELECT {$subqueryFieldList} FROM {$quotedTableName}";
        if ($subqueryConditions !== []) {
            $innerSql .= ' WHERE ' . implode(' AND ', $subqueryConditions);
        }

        $limitBinding = ':' . $bindingPrefix . '_limit';
        $bindings[$limitBinding] = $limit;
        $innerSql .= " LIMIT {$limitBinding}";

        $derivedAlias = self::quoteColumnName($dbType, $bindingPrefix . '_rows');

        if (count($quotedPrimaryKeyFields) === 1) {
            return $quotedTableName . '.' . $quotedPrimaryKeyFields[0]
                . ' IN (SELECT ' . $quotedPrimaryKeyFields[0] . " FROM ({$innerSql}) AS {$derivedAlias})";
        }

        $outerTuple = '(' . implode(', ', array_map(
            static fn(string $field): string => $quotedTableName . '.' . $field,
            $quotedPrimaryKeyFields
        )) . ')';

        return $outerTuple . ' IN (SELECT ' . implode(', ', $quotedPrimaryKeyFields)
            . " FROM ({$innerSql}) AS {$derivedAlias})";
    }

    public static function normalizeGroupByFields(mixed $byRaw, array $fields, string $modelName): array
    {
        if (is_string($byRaw)) {
            $byRaw = [$byRaw];
        }

        if (!$byRaw || !is_array($byRaw)) {
            throw new Exception("'by' must be a non-empty string or array.");
        }

        $by = array_values(array_filter(array_map(
            static fn(mixed $field): string => is_string($field) ? trim($field) : '',
            $byRaw
        ), static fn(string $field): bool => $field !== ''));

        if ($by === []) {
            throw new Exception("'by' cannot contain empty values.");
        }

        foreach ($by as $field) {
            if (!isset($fields[$field])) {
                throw new Exception("Field '$field' does not exist in {$modelName}.");
            }
        }

        return $by;
    }

    public static function normalizeOrderByEntries(mixed $orderBy): array
    {
        if ($orderBy === null) {
            return [];
        }

        if (!is_array($orderBy)) {
            throw new Exception("'orderBy' must be an array or a list of arrays.");
        }

        if ($orderBy === []) {
            return [];
        }

        $entries = array_is_list($orderBy) ? $orderBy : [$orderBy];

        foreach ($entries as $index => $entry) {
            if (!is_array($entry)) {
                throw new Exception("Each 'orderBy' entry must be an array. Invalid entry at index {$index}.");
            }
        }

        return $entries;
    }

    private static function pruneEmptyOrderByEntries(array $orderByEntries): array
    {
        return array_values(array_filter(
            $orderByEntries,
            static fn(array $entry): bool => $entry !== []
        ));
    }

    private static function isValidSortDirection(mixed $direction): bool
    {
        return is_string($direction) && in_array(strtolower($direction), ['asc', 'desc'], true);
    }

    public static function validateGroupByOrderBy(
        array $criteria,
        array $by,
        array $fields = [],
        string $modelName = ''
    ): void {
        $orderByEntries = self::pruneEmptyOrderByEntries(
            self::normalizeOrderByEntries($criteria['orderBy'] ?? null)
        );
        $hasWindowing = array_key_exists('skip', $criteria) || array_key_exists('take', $criteria);

        if ($hasWindowing && $orderByEntries === []) {
            throw new Exception("If you provide 'take' or 'skip', you also need to provide 'orderBy'.");
        }

        if ($orderByEntries === []) {
            return;
        }

        $aggregateKeys = ['_count', '_avg', '_sum', '_min', '_max'];
        $missing = [];

        foreach ($orderByEntries as $entry) {
            if (count($entry) !== 1) {
                throw new Exception('Each groupBy orderBy entry must contain exactly one field.');
            }

            foreach ($entry as $field => $direction) {
                if (in_array($field, $aggregateKeys, true)) {
                    if (!is_array($direction) || $direction === []) {
                        throw new Exception("Aggregate orderBy '$field' must be a non-empty array.");
                    }

                    if (count($direction) !== 1) {
                        throw new Exception("Aggregate orderBy '$field' must contain exactly one field.");
                    }

                    foreach ($direction as $aggregateField => $aggregateDirection) {
                        if ($aggregateField === '_all') {
                            throw new Exception("groupBy orderBy does not support '$field._all'.");
                        }

                        if ($fields !== []) {
                            if (!isset($fields[$aggregateField])) {
                                $targetModel = $modelName !== '' ? $modelName : 'this model';
                                throw new Exception("Field '$aggregateField' does not exist in {$targetModel}.");
                            }

                            if (($fields[$aggregateField]['kind'] ?? null) !== 'scalar') {
                                $targetModel = $modelName !== '' ? $modelName : 'this model';
                                throw new Exception("Field '$aggregateField' is not a scalar field in {$targetModel}.");
                            }
                        }

                        if (!self::isValidSortDirection($aggregateDirection)) {
                            throw new Exception("Invalid sort direction for groupBy orderBy. Expected 'asc' or 'desc'.");
                        }
                    }

                    continue;
                }

                if (!self::isValidSortDirection($direction)) {
                    throw new Exception("Invalid sort direction for groupBy orderBy. Expected 'asc' or 'desc'.");
                }

                if (!in_array($field, $by, true)) {
                    $missing[] = $field;
                }
            }
        }

        if ($missing !== []) {
            throw new Exception(
                'Every field used for orderBy must be included in the by-arguments of the query. Missing fields: '
                    . implode(', ', array_values(array_unique($missing)))
            );
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

    public static function appendRelationCountOrderJoin(
        string $relationName,
        array $field,
        array $fields,
        array $fieldsRelatedWithKeys,
        string $modelName,
        string $primaryKey,
        PDO $pdo,
        string $dbType,
        string $quotedTableName,
        array &$joins,
    ): void {
        if (($field['isList'] ?? false) !== true) {
            throw new Exception("Relation count ordering is only supported for list relation '$relationName'.");
        }

        $aliasQuoted = self::quoteColumnName($dbType, $relationName);
        foreach ($joins as $existingJoin) {
            if (strpos($existingJoin, " AS {$aliasQuoted} ") !== false) {
                return;
            }
        }

        $relatedInstance = self::makeRelatedInstance($field['type'], $pdo);
        $relatedKeys = $fieldsRelatedWithKeys[$relationName] ?? [
            'relationFromFields' => [],
            'relationToFields' => [],
        ];

        if (empty($relatedKeys['relationFromFields']) && empty($relatedKeys['relationToFields'])) {
            $relationInfo = self::resolveImplicitRelationTable($field, $relatedInstance, $modelName);
            $pivotTable = self::quoteColumnName($dbType, $relationInfo['table']);
            $currentColumnQuoted = self::quoteColumnName($dbType, $relationInfo['currentColumn']);
            $countQuoted = self::quoteColumnName($dbType, '_count');
            $primaryKeyQuoted = self::quoteColumnName($dbType, $primaryKey);

            $subquery = "(SELECT {$pivotTable}.{$currentColumnQuoted} AS pivot_main, COUNT(*) AS {$countQuoted} " .
                "FROM {$pivotTable} GROUP BY {$pivotTable}.{$currentColumnQuoted})";

            $joins[] = "LEFT JOIN {$subquery} AS {$aliasQuoted} ON ({$quotedTableName}.{$primaryKeyQuoted} = {$aliasQuoted}.pivot_main)";
            return;
        }

        $instanceField = self::pickOppositeField(
            $relatedInstance->_fields,
            $field['relationName'],
            $field['isList'] ?? false
        );

        $fromFields = $instanceField['relationFromFields'] ?? [];
        $toFields = $instanceField['relationToFields'] ?? [];

        if ($fromFields === [] || $toFields === [] || count($fromFields) !== count($toFields)) {
            throw new Exception("Relation count ordering is not properly defined for '$relationName'.");
        }

        $relatedTable = self::quoteColumnName($dbType, $relatedInstance->_tableName);
        $countQuoted = self::quoteColumnName($dbType, '_count');
        $selectKeyParts = [];
        $groupByParts = [];
        $joinConditions = [];

        foreach ($fromFields as $index => $fromField) {
            $toField = $toFields[$index] ?? null;
            if ($toField === null) {
                throw new Exception("Relation count ordering is missing join metadata for '$relationName'.");
            }

            $keyAlias = "fk_{$index}";
            $keyAliasQuoted = self::quoteColumnName($dbType, $keyAlias);
            $fromQuoted = self::quoteColumnName($dbType, $fromField);
            $toQuoted = self::quoteColumnName($dbType, $toField);

            $selectKeyParts[] = "{$relatedTable}.{$fromQuoted} AS {$keyAliasQuoted}";
            $groupByParts[] = "{$relatedTable}.{$fromQuoted}";
            $joinConditions[] = "{$quotedTableName}.{$toQuoted} = {$aliasQuoted}.{$keyAliasQuoted}";
        }

        $subquery = "(SELECT " . implode(', ', $selectKeyParts) . ", COUNT(*) AS {$countQuoted} " .
            "FROM {$relatedTable} GROUP BY " . implode(', ', $groupByParts) . ')';

        $joins[] = "LEFT JOIN {$subquery} AS {$aliasQuoted} ON (" . implode(' AND ', $joinConditions) . ')';
    }

    private static function parseOrderBy(
        mixed $orderBy,
        string $dbType,
        string $tableName
    ): array {
        $aggKeys = ['_count', '_avg', '_sum', '_min', '_max'];
        $parts   = [];

        foreach (self::normalizeOrderByEntries($orderBy) as $entry) {
            foreach ($entry as $key => $value) {

                if (in_array($key, $aggKeys, true) && is_array($value)) {
                    $sqlFunction = match ($key) {
                        '_count' => 'COUNT',
                        '_avg' => 'AVG',
                        '_sum' => 'SUM',
                        '_min' => 'MIN',
                        '_max' => 'MAX',
                    };

                    foreach ($value as $field => $dir) {
                        $direction = strtolower((string)$dir) === 'desc' ? 'DESC' : 'ASC';

                        if ($key === '_count' && $field === '_all') {
                            $parts[] = $sqlFunction . '(*) ' . $direction;
                            continue;
                        }

                        $quotedField = self::quoteColumnName($dbType, (string)$field);
                        $parts[] = $sqlFunction . '(' . $tableName . '.' . $quotedField . ') ' . $direction;
                    }

                    continue;
                }

                if (is_array($value)) {
                    foreach ($value as $nested => $dir) {
                        $dir = strtolower((string)$dir) === 'desc' ? 'DESC' : 'ASC';
                        if ($nested === '_count') {
                            $parts[] = 'COALESCE(' . self::quoteColumnName($dbType, $key) . '.' .
                                self::quoteColumnName($dbType, $nested) . ", 0) $dir";
                            continue;
                        }

                        $parts[] = self::quoteColumnName($dbType, $key) . '.' .
                            self::quoteColumnName($dbType, $nested) . " $dir";
                    }
                } else {
                    $dir = strtolower((string)$value) === 'desc' ? 'DESC' : 'ASC';
                    $parts[] = "$tableName." . self::quoteColumnName($dbType, $key) . " $dir";
                }
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
            $relatedModelName = $model->_fields[$relationName]['type'] ?? null;
            if (!is_string($relatedModelName) || $relatedModelName === '') {
                throw new Exception("No related model name found for relation '$relationName'.");
            }

            $relatedClass = self::makeRelatedInstance($relatedModelName, $pdo);
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

    private static function resolveRelationRecordId(object $instance, array $selector): mixed
    {
        $primaryKey = $instance->_primaryKey;
        if (isset($selector[$primaryKey])) {
            return $selector[$primaryKey];
        }

        $record = $instance->findUnique(['where' => $selector]);
        if ($record === null) {
            $record = $instance->findFirst(['where' => $selector]);
        }

        if ($record === null) {
            throw new Exception('Cannot resolve an implicit relation record from the provided selector.');
        }

        return $record->{$primaryKey};
    }

    private static function handleImplicitRelationSelect(
        array $field,
        object $relatedInstance,
        string $currentModelName,
        string $dbType,
        PDO $pdo,
        mixed $currentId
    ): array {
        $relationInfo = self::resolveImplicitRelationTable($field, $relatedInstance, $currentModelName);
        $tableName = self::quoteColumnName($dbType, $relationInfo['table']);
        $currentColumnQuoted = self::quoteColumnName($dbType, $relationInfo['currentColumn']);

        $sqlSelect = "SELECT * FROM $tableName WHERE $currentColumnQuoted = ?";
        $stmtSelect = $pdo->prepare($sqlSelect);
        $stmtSelect->execute([$currentId]);
        return $stmtSelect->fetchAll();
    }

    private static function handleImplicitRelationInsert(
        array $field,
        object $relatedInstance,
        string $currentModelName,
        string $dbType,
        PDO $pdo,
        mixed $currentId,
        mixed $relatedId
    ): array {
        $relationInfo = self::resolveImplicitRelationTable($field, $relatedInstance, $currentModelName);
        $tableName = self::quoteColumnName($dbType, $relationInfo['table']);
        $currentColumnQuoted = self::quoteColumnName($dbType, $relationInfo['currentColumn']);
        $relatedColumnQuoted = self::quoteColumnName($dbType, $relationInfo['relatedColumn']);

        if ($dbType === 'mysql') {
            $sql = "INSERT IGNORE INTO $tableName ($currentColumnQuoted, $relatedColumnQuoted) VALUES (?, ?)";
        } elseif ($dbType === 'sqlite') {
            $sql = "INSERT OR IGNORE INTO $tableName ($currentColumnQuoted, $relatedColumnQuoted) VALUES (?, ?)";
        } elseif ($dbType === 'pgsql') {
            $sql = "INSERT INTO $tableName ($currentColumnQuoted, $relatedColumnQuoted) VALUES (?, ?) ON CONFLICT DO NOTHING";
        } else {
            $sql = "INSERT INTO $tableName ($currentColumnQuoted, $relatedColumnQuoted) VALUES (?, ?)";
        }

        $stmt = $pdo->prepare($sql);
        $stmt->execute([$currentId, $relatedId]);

        $sqlSelect = "SELECT * FROM $tableName WHERE $currentColumnQuoted = ? AND $relatedColumnQuoted = ?";
        $stmtSelect = $pdo->prepare($sqlSelect);
        $stmtSelect->execute([$currentId, $relatedId]);

        $result = $stmtSelect->fetch();
        return $result ?: [];
    }

    private static function handleImplicitRelationDelete(
        array $field,
        object $relatedInstance,
        string $currentModelName,
        string $dbType,
        PDO $pdo,
        mixed $currentId,
        array $relatedIds,
        bool $deleteMatchingIds
    ): void {
        $relationInfo = self::resolveImplicitRelationTable($field, $relatedInstance, $currentModelName);
        $tableName = self::quoteColumnName($dbType, $relationInfo['table']);
        $currentColumnQuoted = self::quoteColumnName($dbType, $relationInfo['currentColumn']);
        $relatedColumnQuoted = self::quoteColumnName($dbType, $relationInfo['relatedColumn']);

        if ($relatedIds === []) {
            if ($deleteMatchingIds) {
                return;
            }

            $sqlDelete = "DELETE FROM $tableName WHERE $currentColumnQuoted = ?";
            $stmtDelete = $pdo->prepare($sqlDelete);
            $stmtDelete->execute([$currentId]);
            return;
        }

        $placeholders = implode(',', array_fill(0, count($relatedIds), '?'));
        $operator = $deleteMatchingIds ? 'IN' : 'NOT IN';
        $sqlDelete = "DELETE FROM $tableName WHERE $currentColumnQuoted = ? AND $relatedColumnQuoted $operator ($placeholders)";
        $stmtDelete = $pdo->prepare($sqlDelete);
        $stmtDelete->execute(array_merge([$currentId], $relatedIds));
    }

    public static function processRelation(
        string $modelName,
        string $relatedFieldName,
        array  $fieldData,
        PDO    $pdo,
        string $dbType,
        bool   $requestOption = true,
    ): array {
        $modelClass = self::makeRelatedInstance($modelName, $pdo);

        $modelFieldsRelatedWithKeys = $modelClass->_fieldsRelatedWithKeys[$relatedFieldName];
        $modelRelatedFromFields     = $modelFieldsRelatedWithKeys['relationFromFields'];
        $modelRelatedToFields       = $modelFieldsRelatedWithKeys['relationToFields'];

        $modelRelatedField       = $modelClass->_fields[$relatedFieldName];
        $modelRelatedFieldIsList = $modelRelatedField['isList'] ?? false;
        $modelRelatedFieldType   = $modelRelatedField['type'];

        $relatedClass = self::makeRelatedInstance($modelRelatedFieldType, $pdo);

        $inverseInfo      = null;
        $inverseFieldName = null;
        $childFkFields    = [];
        foreach ($relatedClass->_fieldsRelatedWithKeys as $childField => $info) {
            $childFieldMeta = $relatedClass->_fields[$childField] ?? [];
            $infoType = $info['type'] ?? ($childFieldMeta['type'] ?? null);
            $sameRelationName = ($childFieldMeta['relationName'] ?? null) === ($modelRelatedField['relationName'] ?? null);

            if ($sameRelationName && $infoType === $modelName && !empty($info['relationFromFields'])) {
                $inverseInfo   = $info;
                $inverseFieldName = $childField;
                $childFkFields = $info['relationFromFields'];
                break;
            }
        }
        $isExplicitOneToMany = $modelRelatedFieldIsList && $inverseInfo !== null;
        $isBelongsToRelation = !$modelRelatedFieldIsList
            && !empty($modelRelatedFromFields)
            && array_key_exists($modelRelatedFromFields[0], $modelClass->_fields);
        $isInverseToOne = !$modelRelatedFieldIsList
            && !empty($modelRelatedFromFields)
            && !empty($modelRelatedToFields)
            && !$isBelongsToRelation;

        $relatedResult = null;

        foreach ($fieldData as $action => $actionData) {
            $operations = isset($actionData[0]) ? $actionData : [$actionData];

            foreach ($operations as $op) {
                switch ($action) {
                    case 'connect':
                        if ($isExplicitOneToMany) {
                            $payload = self::unwrapExplicitOneToManyPayload($op);
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $childFkFields,
                                $inverseInfo['relationToFields'] ?? [],
                                $relatedFieldName
                            );

                            if (isset($op['__related']) && is_array($op['__related'])) {
                                $where = self::resolveRecordSelector($relatedClass, $payload);
                                if ($where === []) {
                                    $where = array_diff_key($payload, array_flip($childFkFields));
                                }
                            } else {
                                $where = array_diff_key($payload, array_flip($childFkFields));
                                if ($where === []) {
                                    $where = self::resolveRecordSelector($relatedClass, $payload);
                                }
                            }
                            if (!$where) {
                                throw new Exception("A unique selector (e.g. 'code') is required inside 'connect' for '{$relatedFieldName}'.");
                            }

                            $parentSelector = [];
                            foreach (($inverseInfo['relationToFields'] ?? []) as $index => $parentKeyField) {
                                $childFkField = $childFkFields[$index] ?? null;
                                if ($childFkField !== null && $parentKeyField !== null && array_key_exists($childFkField, $parentReference)) {
                                    $parentSelector[$parentKeyField] = $parentReference[$childFkField];
                                }
                            }

                            if ($inverseFieldName !== null && $parentSelector !== []) {
                                $relatedResult = $relatedClass->update([
                                    'where' => $where,
                                    'data'  => [
                                        $inverseFieldName => [
                                            'connect' => $parentSelector,
                                        ],
                                    ],
                                ]);
                            } else {
                                $relatedResult = $relatedClass->update([
                                    'where' => $where,
                                    'data'  => $parentReference,
                                ]);
                            }
                        } elseif ($isInverseToOne) {
                            $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $modelRelatedFromFields,
                                $modelRelatedToFields,
                                $relatedFieldName
                            );

                            $where = array_diff_key($payload, array_flip($modelRelatedFromFields));
                            if ($where === []) {
                                $where = self::resolveRecordSelector($relatedClass, $payload);
                            }

                            if ($where === []) {
                                throw new Exception("A unique selector is required inside 'connect' for '{$relatedFieldName}'.");
                            }

                            $relatedResult = $relatedClass->update([
                                'where' => $where,
                                'data' => $parentReference,
                            ]);
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $relatedFieldData = $op['__related'] ?? $op[$modelRelatedFieldType];
                            $modelFieldData   = $op['__parent'] ?? $op[$modelName];

                            $relatedId = self::resolveRelationRecordId($relatedClass, $relatedFieldData);
                            $modelId   = self::resolveRelationRecordId($modelClass, $modelFieldData);

                            $relatedResult = self::handleImplicitRelationInsert(
                                $modelRelatedField,
                                $relatedClass,
                                $modelName,
                                $dbType,
                                $pdo,
                                $modelId,
                                $relatedId
                            );
                        } else {
                            if (!$modelRelatedFieldIsList && count($operations) > 1) {
                                throw new Exception("Cannot connect multiple records for a non-list relation '{$relatedFieldName}'.");
                            }

                            $connectQuery = ['where' => $op];
                            if ($requestOption && !empty($modelRelatedToFields)) {
                                $connectQuery['select'] = array_fill_keys($modelRelatedToFields, true);
                            }

                            $relatedResult = $relatedClass->findUnique($connectQuery);
                        }
                        break;
                    case 'connectOrCreate':
                        if ($isExplicitOneToMany) {
                            if (!$modelRelatedFieldIsList && count($operations) > 1) {
                                throw new Exception("Cannot connectOrCreate multiple records for a non-list relation '$relatedFieldName'.");
                            }

                            $payload = self::unwrapExplicitOneToManyPayload($op);
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $childFkFields,
                                $inverseInfo['relationToFields'] ?? [],
                                $relatedFieldName
                            );

                            $where = $payload['where']
                                ?? throw new Exception("connectOrCreate requires 'where' for '{$relatedFieldName}'.");
                            $createData = $payload['create']
                                ?? throw new Exception("connectOrCreate requires 'create' for '{$relatedFieldName}'.");

                            foreach ($parentReference as $field => $value) {
                                $createData[$field] = $value;
                            }

                            $existing = $relatedClass->findUnique(['where' => $where]);

                            if ($existing) {
                                $relatedResult = $relatedClass->update([
                                    'where' => $where,
                                    'data' => $parentReference,
                                ]);
                            } else {
                                $relatedResult = $relatedClass->create(['data' => $createData]);
                            }
                        } elseif ($isInverseToOne) {
                            $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $modelRelatedFromFields,
                                $modelRelatedToFields,
                                $relatedFieldName
                            );

                            $where = $payload['where']
                                ?? throw new Exception("connectOrCreate requires 'where' for '{$relatedFieldName}'.");
                            $createData = $payload['create']
                                ?? throw new Exception("connectOrCreate requires 'create' for '{$relatedFieldName}'.");

                            foreach ($parentReference as $field => $value) {
                                $createData[$field] = $value;
                            }

                            $existing = $relatedClass->findUnique(['where' => $where]);

                            if ($existing) {
                                $relatedResult = $relatedClass->update([
                                    'where' => $where,
                                    'data' => $parentReference,
                                ]);
                            } else {
                                $relatedResult = $relatedClass->create(['data' => $createData]);
                            }
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $relatedFieldData = $op['__related'] ?? $op[$modelRelatedFieldType];
                            $modelFieldData = $op['__parent'] ?? $op[$modelName];
                            $existingRecord = $relatedClass->findFirst(['where' => $relatedFieldData['where']]);
                            $modelId = self::resolveRelationRecordId($modelClass, $modelFieldData);

                            if ($existingRecord) {
                                $record = $existingRecord;
                            } else {
                                $record = $relatedClass->create(['data' => $relatedFieldData['create']]);
                            }

                            $relatedResult = self::handleImplicitRelationInsert(
                                $modelRelatedField,
                                $relatedClass,
                                $modelName,
                                $dbType,
                                $pdo,
                                $modelId,
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
                        if ($isExplicitOneToMany) {
                            if (!$modelRelatedFieldIsList && count($operations) > 1) {
                                throw new Exception("Cannot create multiple records for a non-list relation '$relatedFieldName'.");
                            }

                            $payload = self::unwrapExplicitOneToManyPayload($op);
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $childFkFields,
                                $inverseInfo['relationToFields'] ?? [],
                                $relatedFieldName
                            );
                            foreach ($parentReference as $field => $value) {
                                $payload[$field] = $value;
                            }
                            $relatedResult = $relatedClass->create(['data' => $payload]);
                        } elseif ($isInverseToOne) {
                            if (count($operations) > 1) {
                                throw new Exception("Cannot create multiple records for a non-list relation '$relatedFieldName'.");
                            }

                            $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $modelRelatedFromFields,
                                $modelRelatedToFields,
                                $relatedFieldName
                            );
                            foreach ($parentReference as $field => $value) {
                                $payload[$field] = $value;
                            }
                            $relatedResult = $relatedClass->create(['data' => $payload]);
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $relatedFieldData = $op['__related'] ?? $op[$modelRelatedFieldType];
                            $modelFieldData = $op['__parent'] ?? $op[$modelName];
                            $modelId = self::resolveRelationRecordId($modelClass, $modelFieldData);
                            $relatedCreatedData = $relatedClass->create(['data' => $relatedFieldData]);
                            $relatedResult = self::handleImplicitRelationInsert(
                                $modelRelatedField,
                                $relatedClass,
                                $modelName,
                                $dbType,
                                $pdo,
                                $modelId,
                                $relatedCreatedData->{$relatedClass->_primaryKey},
                            );
                        } else {
                            if (!$modelRelatedFieldIsList && count($operations) > 1) {
                                throw new Exception("Cannot create multiple records for a non-list relation '$relatedFieldName'.");
                            }

                            $relatedResult = $relatedClass->create(['data' => $op]);
                        }
                        break;
                    case 'createMany':
                        if (!$modelRelatedFieldIsList) {
                            throw new Exception("createMany is only supported for list relations.");
                        }

                        if (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            throw new Exception("createMany is not supported for implicit many-to-many relations.");
                        }

                        $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                        $rows = $payload['data']
                            ?? throw new Exception("createMany requires 'data' for '{$relatedFieldName}'.");

                        if (!is_array($rows) || !array_is_list($rows)) {
                            throw new Exception("createMany requires 'data' to be a list of records for '{$relatedFieldName}'.");
                        }

                        $parentReference = self::resolveExplicitParentReference(
                            $op,
                            !empty($childFkFields) ? $childFkFields : $modelRelatedFromFields,
                            !empty($inverseInfo['relationToFields'] ?? []) ? ($inverseInfo['relationToFields'] ?? []) : $modelRelatedToFields,
                            $relatedFieldName
                        );

                        $rows = array_map(
                            static fn(array $row) => array_merge($row, $parentReference),
                            $rows
                        );

                        $createManyArgs = ['data' => $rows];
                        if (array_key_exists('skipDuplicates', $payload)) {
                            $createManyArgs['skipDuplicates'] = (bool) $payload['skipDuplicates'];
                        }

                        $relatedClass->createMany($createManyArgs);
                        $relatedResult = true;
                        break;
                    case 'delete':
                        if ($isExplicitOneToMany) {
                            $payload = self::unwrapExplicitOneToManyPayload($op);
                            $whereCondition = self::resolveOperationWhere($relatedClass, $payload, $relatedFieldName, 'delete');
                            $relatedResult = $relatedClass->delete(['where' => $whereCondition]);
                        } elseif ($isInverseToOne) {
                            $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $modelRelatedFromFields,
                                $modelRelatedToFields,
                                $relatedFieldName
                            );
                            $nestedWhere = isset($payload['where']) && is_array($payload['where'])
                                ? $payload['where']
                                : array_diff_key($payload, array_flip($modelRelatedFromFields));
                            $where = self::mergeWhereClauses($parentReference, $nestedWhere);

                            $existing = $relatedClass->findFirst(['where' => $where]);
                            if ($existing === null) {
                                throw new Exception("No {$relatedClass->_modelName} record found matching the criteria.");
                            }

                            $recordSelector = [];
                            if (!empty($relatedClass->_primaryKey) && isset($existing->{$relatedClass->_primaryKey})) {
                                $recordSelector = [$relatedClass->_primaryKey => $existing->{$relatedClass->_primaryKey}];
                            } else {
                                $recordSelector = self::resolveRecordSelector($relatedClass, (array) $existing);
                            }

                            $relatedClass->delete(['where' => $recordSelector]);
                            $relatedResult = true;
                        } else {
                            $whereCondition = $op[$modelRelatedFieldType];
                            $relatedResult = $relatedClass->delete(['where' => $whereCondition]);
                        }
                        break;
                    case 'deleteMany':
                        if ($isExplicitOneToMany) {
                            foreach ($operations as $opDelete) {
                                $payload = self::unwrapExplicitOneToManyPayload($opDelete);
                                if (!isset($payload['where'])) {
                                    throw new Exception("deleteMany requires 'where' for '{$relatedFieldName}'.");
                                }

                                $where = $payload['where'];

                                if (!empty($childFkFields)) {
                                    $parentReference = self::resolveExplicitParentReference(
                                        $opDelete,
                                        $childFkFields,
                                        $inverseInfo['relationToFields'] ?? [],
                                        $relatedFieldName
                                    );
                                    foreach ($parentReference as $field => $value) {
                                        $where[$field] = $value;
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
                            if (self::tryFastExplicitOneToManyDisconnect(
                                $relatedClass,
                                $pdo,
                                $dbType,
                                $childFkFields,
                                $operations
                            )) {
                                $relatedResult = true;
                                break;
                            }

                            foreach ($operations as $opDisc) {
                                $payload = self::unwrapExplicitOneToManyPayload($opDisc);
                                $where = array_diff_key($payload, array_flip($childFkFields));
                                if ($where === []) {
                                    $where = self::resolveRecordSelector($relatedClass, $payload);
                                }
                                $relatedClass->update([
                                    'where' => $where,
                                    'data'  => array_fill_keys($childFkFields, null),
                                ]);
                            }
                            $relatedResult = true;
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $rData = $op['__related'] ?? $op[$modelRelatedFieldType];
                            $mData = $op['__parent'] ?? $op[$modelName];
                            $currentId = self::resolveRelationRecordId($modelClass, $mData);
                            $relatedId = self::resolveRelationRecordId($relatedClass, $rData);
                            self::handleImplicitRelationDelete(
                                $modelRelatedField,
                                $relatedClass,
                                $modelName,
                                $dbType,
                                $pdo,
                                $currentId,
                                [$relatedId],
                                true
                            );
                            $relatedResult = true;
                        } elseif ($isInverseToOne) {
                            $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $modelRelatedFromFields,
                                $modelRelatedToFields,
                                $relatedFieldName
                            );
                            foreach ($modelRelatedFromFields as $fromField) {
                                if (($relatedClass->_fields[$fromField]['isRequired'] ?? false) === true) {
                                    throw new Exception("Cannot disconnect required relation '{$relatedFieldName}'.");
                                }
                            }
                            $nestedWhere = isset($payload['where']) && is_array($payload['where'])
                                ? $payload['where']
                                : array_diff_key($payload, array_flip($modelRelatedFromFields));
                            $where = self::mergeWhereClauses($parentReference, $nestedWhere);

                            $relatedClass->updateMany([
                                'where' => $where,
                                'data' => array_fill_keys($modelRelatedFromFields, null),
                            ]);
                            $relatedResult = true;
                        } else {
                            $relatedResult = $relatedClass->delete(['where' => $op]);
                        }
                        break;
                    case 'set':
                        if ($isExplicitOneToMany) {
                            if (empty($operations)) {
                                return [];
                            }

                            if (self::tryFastExplicitOneToManySet(
                                $relatedClass,
                                $pdo,
                                $dbType,
                                $childFkFields,
                                $inverseInfo['relationToFields'] ?? [],
                                $relatedFieldName,
                                $operations
                            )) {
                                return [];
                            }

                            $parentReference = self::resolveExplicitParentReference(
                                $operations[0],
                                $childFkFields,
                                $inverseInfo['relationToFields'] ?? [],
                                $relatedFieldName
                            );
                            $parentId = $parentReference[$childFkFields[0]]
                                ?? throw new Exception("Missing parent id in 'set' for '{$relatedFieldName}'.");

                            $primaryKey = $relatedClass->_primaryKey;
                            $attachedIds = [];

                            foreach ($operations as $opSet) {
                                $payload = self::unwrapExplicitOneToManyPayload($opSet);
                                $currentParentReference = self::resolveExplicitParentReference(
                                    $opSet,
                                    $childFkFields,
                                    $inverseInfo['relationToFields'] ?? [],
                                    $relatedFieldName
                                );
                                $selector = self::resolveRecordSelector($relatedClass, $payload);

                                if ($selector === []) {
                                    throw new Exception("Cannot determine unique identifier for '{$relatedFieldName}'. No key fields found.");
                                }

                                $existing = $relatedClass->findUnique(['where' => $selector]);
                                if ($existing) {
                                    $relatedClass->update([
                                        'where' => $selector,
                                        'data' => $currentParentReference,
                                    ]);
                                    if ($primaryKey !== '' && isset($existing->{$primaryKey})) {
                                        $attachedIds[] = $existing->{$primaryKey};
                                    }
                                } else {
                                    $created = $relatedClass->create([
                                        'data' => array_merge($payload, $currentParentReference),
                                    ]);
                                    if ($primaryKey !== '' && isset($created->{$primaryKey})) {
                                        $attachedIds[] = $created->{$primaryKey};
                                    }
                                }
                            }

                            if ($primaryKey === '') {
                                throw new Exception("The 'set' operation for '{$relatedFieldName}' requires a primary key on the related model.");
                            }

                            $childFieldMeta = $relatedClass->_fields[$childFkFields[0]] ?? [];
                            $detachWhere = [$childFkFields[0] => $parentId];
                            if ($attachedIds !== []) {
                                $detachWhere[$primaryKey] = ['notIn' => $attachedIds];
                            }

                            if (($childFieldMeta['isRequired'] ?? false) === false) {
                                $relatedClass->updateMany([
                                    'where' => $detachWhere,
                                    'data' => [$childFkFields[0] => null],
                                ]);
                            } else {
                                $relatedClass->deleteMany(['where' => $detachWhere]);
                            }

                            return [];
                        } elseif (empty($modelRelatedFromFields) && empty($modelRelatedToFields)) {
                            $newRelatedIds = [];
                            $primaryId = null;

                            foreach ($operations as $opSet) {
                                $relatedFieldData = $opSet['__related'] ?? $opSet[$modelRelatedFieldType];
                                $modelFieldData   = $opSet['__parent'] ?? $opSet[$modelName];
                                $newRelatedIds[]  = self::resolveRelationRecordId($relatedClass, $relatedFieldData);
                                if (!$primaryId) {
                                    $primaryId = self::resolveRelationRecordId($modelClass, $modelFieldData);
                                }
                            }
                            $newRelatedIds = array_unique($newRelatedIds);

                            self::handleImplicitRelationDelete(
                                $modelRelatedField,
                                $relatedClass,
                                $modelName,
                                $dbType,
                                $pdo,
                                $primaryId,
                                $newRelatedIds,
                                false
                            );

                            foreach ($newRelatedIds as $relatedId) {
                                self::handleImplicitRelationInsert(
                                    $modelRelatedField,
                                    $relatedClass,
                                    $modelName,
                                    $dbType,
                                    $pdo,
                                    $primaryId,
                                    $relatedId
                                );
                            }

                            $relatedResult = self::handleImplicitRelationSelect(
                                $modelRelatedField,
                                $relatedClass,
                                $modelName,
                                $dbType,
                                $pdo,
                                $primaryId
                            );
                        } else {
                            $relatedResult = $relatedClass->findUnique(['where' => $op]);
                        }
                        break;
                    case 'update':
                        if ($isExplicitOneToMany) {
                            $payload = self::unwrapExplicitOneToManyPayload($op);
                            $where = self::resolveOperationWhere($relatedClass, $payload, $relatedFieldName, 'update');
                            $data = $payload['data']
                                ?? throw new Exception("update requires 'data' for '{$relatedFieldName}'.");

                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $childFkFields,
                                $inverseInfo['relationToFields'] ?? [],
                                $relatedFieldName
                            );
                            foreach ($parentReference as $field => $value) {
                                $data[$field] = $value;
                            }

                            $relatedResult = $relatedClass->update([
                                'where' => $where,
                                'data' => $data,
                            ]);
                        } elseif ($isInverseToOne) {
                            $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $modelRelatedFromFields,
                                $modelRelatedToFields,
                                $relatedFieldName
                            );

                            if (isset($payload['data']) && is_array($payload['data'])) {
                                $nestedWhere = isset($payload['where']) && is_array($payload['where'])
                                    ? $payload['where']
                                    : [];
                                $where = self::mergeWhereClauses($parentReference, $nestedWhere);
                                $data = $payload['data'];
                            } else {
                                $where = $parentReference;
                                $data = array_diff_key($payload, array_flip($modelRelatedFromFields));
                            }

                            foreach ($parentReference as $field => $value) {
                                $data[$field] = $value;
                            }

                            $relatedResult = $relatedClass->update([
                                'where' => $where,
                                'data' => $data,
                            ]);
                        } elseif (!empty($modelRelatedFromFields) && !empty($modelRelatedToFields)) {
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

                            $parentReference = self::resolveExplicitParentReference(
                                $operations[0],
                                $childFkFields,
                                $inverseInfo['relationToFields'] ?? [],
                                $relatedFieldName
                            );
                            $parentId = $parentReference[$childFkFields[0]] ?? null;
                            if ($parentId === null) {
                                throw new Exception("Missing parent id in 'updateMany' for '{$relatedFieldName}'.");
                            }

                            $fieldUpdates = [];
                            $ids = [];

                            foreach ($operations as $opUpdate) {
                                $payload = self::unwrapExplicitOneToManyPayload($opUpdate);
                                $where = $payload['where']
                                    ?? throw new Exception("updateMany requires 'where' for '{$relatedFieldName}'.");
                                $data = $payload['data']
                                    ?? throw new Exception("updateMany requires 'data' for '{$relatedFieldName}'.");

                                $record = $relatedClass->findFirst([
                                    'where' => array_merge($where, $parentReference)
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

                            foreach ($operations as $opUpsert) {
                                $payload = self::unwrapExplicitOneToManyPayload($opUpsert);
                                $parentReference = self::resolveExplicitParentReference(
                                    $opUpsert,
                                    $childFkFields,
                                    $inverseInfo['relationToFields'] ?? [],
                                    $relatedFieldName
                                );

                                $where = $payload['where']
                                    ?? throw new Exception("upsert requires 'where' for '{$relatedFieldName}'.");
                                $createData = $payload['create']
                                    ?? throw new Exception("upsert requires 'create' for '{$relatedFieldName}'.");
                                $updateData = $payload['update']
                                    ?? throw new Exception("upsert requires 'update' for '{$relatedFieldName}'.");

                                foreach ($parentReference as $field => $value) {
                                    $createData[$field] = $value;
                                    $updateData[$field] = $value;
                                }

                                if (self::tryFastExplicitOneToManyUpsert(
                                    $relatedClass,
                                    $pdo,
                                    $dbType,
                                    $where,
                                    $updateData
                                )) {
                                    continue;
                                }

                                $relatedClass->upsert([
                                    'where' => $where,
                                    'create' => $createData,
                                    'update' => $updateData,
                                ]);
                            }

                            return [];
                        } elseif ($isInverseToOne) {
                            if (empty($operations)) {
                                return [];
                            }

                            $payload = self::stripInternalRelationMarkers(self::unwrapExplicitOneToManyPayload($op));
                            $parentReference = self::resolveExplicitParentReference(
                                $op,
                                $modelRelatedFromFields,
                                $modelRelatedToFields,
                                $relatedFieldName
                            );

                            $createData = $payload['create']
                                ?? throw new Exception("upsert requires 'create' for '{$relatedFieldName}'.");
                            $updateData = $payload['update']
                                ?? throw new Exception("upsert requires 'update' for '{$relatedFieldName}'.");

                            foreach ($parentReference as $field => $value) {
                                $createData[$field] = $value;
                                $updateData[$field] = $value;
                            }

                            $explicitWhere = isset($payload['where']) && is_array($payload['where'])
                                ? $payload['where']
                                : [];
                            $existingWhere = $explicitWhere === []
                                ? $parentReference
                                : self::mergeWhereClauses($parentReference, $explicitWhere);
                            $existing = $relatedClass->findFirst(['where' => $existingWhere]);

                            if ($existing) {
                                $recordSelector = [];
                                if (!empty($relatedClass->_primaryKey) && isset($existing->{$relatedClass->_primaryKey})) {
                                    $recordSelector = [$relatedClass->_primaryKey => $existing->{$relatedClass->_primaryKey}];
                                } else {
                                    $recordSelector = self::resolveRecordSelector($relatedClass, (array) $existing);
                                }

                                $relatedResult = $relatedClass->update([
                                    'where' => $recordSelector,
                                    'data' => $updateData,
                                ]);
                            } else {
                                $relatedResult = $relatedClass->create(['data' => $createData]);
                            }
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
        string $currentModelName = '',
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
                }

                $batchedCountsByRelation = [];
                if (!$isSingle) {
                    foreach ($aggregateOptions['select'] as $relationName => $enabled) {
                        if (!$enabled || !isset($fields[$relationName], $fieldsRelatedWithKeys[$relationName])) {
                            continue;
                        }

                        $countWhere = [];
                        if (is_array($enabled) && isset($enabled['where']) && is_array($enabled['where'])) {
                            $countWhere = $enabled['where'];
                        }

                        $batchedCounts = self::countRelatedRecordsBatch(
                            $records,
                            $fields[$relationName],
                            $fieldsRelatedWithKeys[$relationName],
                            $pdo,
                            $dbType,
                            $countWhere,
                            $currentModelName
                        );

                        if ($batchedCounts !== null) {
                            $batchedCountsByRelation[$relationName] = $batchedCounts;
                        }
                    }
                }

                foreach ($records as $idx => $record) {
                    foreach ($aggregateOptions['select'] as $relationName => $enabled) {
                        if (!$enabled || !isset($fields[$relationName], $fieldsRelatedWithKeys[$relationName])) {
                            continue;
                        }

                        $countWhere = [];
                        if (is_array($enabled) && isset($enabled['where']) && is_array($enabled['where'])) {
                            $countWhere = $enabled['where'];
                        }

                        $count = $batchedCountsByRelation[$relationName][$idx]
                            ?? self::countRelatedRecords(
                                $record,
                                $relationName,
                                $fields[$relationName],
                                $fieldsRelatedWithKeys[$relationName],
                                $pdo,
                                $dbType,
                                $countWhere,
                                $currentModelName
                            );

                        $records[$idx][$virtualField][$relationName] = $count;
                    }
                }
            }
        }

        foreach ($records as $idx => $record) {
            foreach ($virtualFields as $virtualField) {
                if (isset($record[$virtualField]) && is_array($record[$virtualField])) {
                    $records[$idx][$virtualField] = (object) $record[$virtualField];
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
                if (count($instanceField['relationFromFields'] ?? []) > 1 || count($instanceField['relationToFields'] ?? []) > 1) {
                    goto PER_RECORD;
                }

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

            if ($relatedField['isList'] && $instanceField['isList'] && self::isImplicitManyToMany($relatedKeys, $instanceField)) {
                [$base, $where] = self::buildQueryOptions(
                    [],
                    $relationOpts,
                    $relatedField,
                    $relatedKeys,
                    $instanceField
                );

                $grouped = self::loadImplicitManyBatch(
                    $relatedInstance,
                    $relatedField,
                    $instanceField,
                    $records,
                    $base,
                    $where,
                    $currentModelName,
                    $dbType,
                    $pdo,
                );

                if ($grouped !== null) {
                    foreach ($records as $recordIndex => &$rec) {
                        $rec[$relationName] = $grouped[$recordIndex] ?? [];
                    }
                    unset($rec);
                    continue;
                }
            }

            if (
                !$relatedField['isList']
                && !$instanceField['isList']
                && $currentModelName !== $relatedField['type']
            ) {
                $batchLookup = self::resolveToOneBatchLookup($relatedField, $instanceField);
                if ($batchLookup !== null) {
                    $parentField = $batchLookup['parentField'];
                    $lookupField = $batchLookup['lookupField'];
                    $parentValues = array_values(
                        array_unique(
                            array_filter(
                                array_map(
                                    static fn(array $singleRecord): mixed => $singleRecord[$parentField] ?? null,
                                    $records
                                ),
                                static fn(mixed $value): bool => $value !== null
                            )
                        )
                    );

                    if (!$parentValues) {
                        foreach ($records as &$rec) {
                            $rec[$relationName] = null;
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

                    $grouped = self::loadOneToOneBatch($relatedInstance, $lookupField, $parentValues, $base);

                    foreach ($records as &$rec) {
                        $lookupValue = $rec[$parentField] ?? null;
                        $rec[$relationName] = $lookupValue === null
                            ? null
                            : ($grouped[$lookupValue] ?? null);
                    }
                    unset($rec);
                    continue;
                }
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
                    $result = self::isImplicitManyToMany($relatedKeys, $instanceField)
                        ? self::loadImplicitMany($relatedInstance, $relatedField, $instanceField, $singleRecord, $baseQuery, $where, $dbType, $pdo)
                        : self::loadExplicitMany($relatedInstance, $relatedField, $instanceField, $singleRecord, $baseQuery, $fields);
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

    private static function countWhereUsesRelationFilters(array $where, array $fields): bool
    {
        foreach ($where as $key => $value) {
            if (in_array($key, ['AND', 'OR', 'NOT'], true)) {
                if (!is_array($value)) {
                    continue;
                }

                $operands = array_is_list($value) ? $value : [$value];
                foreach ($operands as $operand) {
                    if (is_array($operand) && self::countWhereUsesRelationFilters($operand, $fields)) {
                        return true;
                    }
                }

                continue;
            }

            if (($fields[$key]['kind'] ?? null) === 'object') {
                return true;
            }
        }

        return false;
    }

    private static function mergeWhereClauses(array ...$clauses): array
    {
        $clauses = array_values(array_filter($clauses, static fn(array $clause): bool => $clause !== []));

        return match (count($clauses)) {
            0 => [],
            1 => $clauses[0],
            default => ['AND' => $clauses],
        };
    }

    private static function countLookupKey(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('c');
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        return (string) $value;
    }

    private static function initializeCountMap(array $records): array
    {
        return array_fill_keys(array_keys($records), 0);
    }

    private static function prepareCountFilterRecords(array $records, array $countWhere, object $modelInstance): array
    {
        $normalizedRecords = array_map(
            static fn(mixed $record): mixed => self::normalizeCountFilterValue($record),
            $records
        );

        if ($countWhere === [] || $normalizedRecords === []) {
            return $normalizedRecords;
        }

        $includeTree = self::buildCountFilterIncludesForModel($countWhere, $modelInstance);
        if ($includeTree === []) {
            return $normalizedRecords;
        }

        $normalizedRecords = self::populateIncludedRelations(
            $normalizedRecords,
            $includeTree,
            $modelInstance->_fields,
            $modelInstance->_fieldsRelatedWithKeys,
            $modelInstance->_pdo,
            $modelInstance->_dbType,
            $modelInstance->_modelName,
        );

        return self::normalizeCountFilterValue($normalizedRecords);
    }

    private static function countRelatedRecordsBatch(
        array $records,
        array $relatedField,
        array $relatedKeys,
        PDO $pdo,
        string $dbType,
        array $countWhere = [],
        string $currentModelName = ''
    ): ?array {
        if (count($records) < 2 || !($relatedField['isList'] ?? false)) {
            return null;
        }

        $relatedInstance = self::makeRelatedInstance($relatedField['type'], $pdo);
        $instanceField = self::pickOppositeField(
            $relatedInstance->_fields,
            $relatedField['relationName'],
            $relatedField['isList']
        );

        $counts = self::initializeCountMap($records);

        if (
            !$instanceField['isList']
            && count($instanceField['relationFromFields'] ?? []) === 1
            && count($instanceField['relationToFields'] ?? []) === 1
        ) {
            $childFk = $instanceField['relationFromFields'][0] ?? null;
            $parentPk = $instanceField['relationToFields'][0] ?? null;

            if (!is_string($childFk) || !is_string($parentPk)) {
                return null;
            }

            $parentValues = [];
            $recordIndexesByParent = [];
            foreach ($records as $idx => $record) {
                $parentValue = $record[$parentPk] ?? null;
                if ($parentValue === null) {
                    continue;
                }

                $lookupKey = self::countLookupKey($parentValue);
                if ($lookupKey === null) {
                    continue;
                }

                $parentValues[$lookupKey] = $parentValue;
                $recordIndexesByParent[$lookupKey][] = $idx;
            }

            if ($parentValues === []) {
                return $counts;
            }

            $countWhereUsesRelationFilters = $countWhere !== []
                && self::countWhereUsesRelationFilters($countWhere, $relatedInstance->_fields);

            $queryWhere = [$childFk => ['in' => array_values($parentValues)]];
            if (!$countWhereUsesRelationFilters && $countWhere !== []) {
                $queryWhere = self::mergeWhereClauses($queryWhere, $countWhere);
            }

            $candidateRows = $relatedInstance->findMany(['where' => $queryWhere]);
            if ($candidateRows === []) {
                return $counts;
            }

            $preparedRows = $countWhereUsesRelationFilters
                ? self::prepareCountFilterRecords($candidateRows, $countWhere, $relatedInstance)
                : self::normalizeCountFilterValue($candidateRows);

            $countsByParent = [];
            foreach ($preparedRows as $preparedRow) {
                if (!is_array($preparedRow)) {
                    continue;
                }

                $lookupKey = self::countLookupKey($preparedRow[$childFk] ?? null);
                if ($lookupKey === null || !isset($recordIndexesByParent[$lookupKey])) {
                    continue;
                }

                if (
                    !$countWhereUsesRelationFilters
                    || self::countRecordMatchesWhere($preparedRow, $countWhere, $relatedInstance)
                ) {
                    $countsByParent[$lookupKey] = ($countsByParent[$lookupKey] ?? 0) + 1;
                }
            }

            foreach ($recordIndexesByParent as $lookupKey => $indexes) {
                $count = $countsByParent[$lookupKey] ?? 0;
                foreach ($indexes as $idx) {
                    $counts[$idx] = $count;
                }
            }

            return $counts;
        }

        if (!self::isImplicitManyToMany($relatedKeys, $instanceField)) {
            return null;
        }

        $parentIds = [];
        $recordIndexesByParent = [];
        foreach ($records as $idx => $record) {
            $parentId = self::resolveImplicitCountRecordId($record);
            $lookupKey = self::countLookupKey($parentId);
            if ($lookupKey === null) {
                continue;
            }

            $parentIds[$lookupKey] = $parentId;
            $recordIndexesByParent[$lookupKey][] = $idx;
        }

        if ($parentIds === []) {
            return $counts;
        }

        $isImplicitSelfRelation = $currentModelName !== ''
            && $currentModelName === $relatedInstance->_modelName;

        if ($isImplicitSelfRelation) {
            $relationInfo = self::resolveImplicitRelationTable(
                $relatedField,
                $relatedInstance,
                $relatedInstance->_modelName
            );
        } else {
            if ($currentModelName === '') {
                return null;
            }

            $implicitModelInfo = self::compareStringsAlphabetically($currentModelName, $relatedField['type']);
            $currentColumn = $currentModelName === $implicitModelInfo['A'] ? 'A' : 'B';
            $relationInfo = [
                'table' => $implicitModelInfo['Name'],
                'currentColumn' => $currentColumn,
                'relatedColumn' => $currentColumn === 'A' ? 'B' : 'A',
            ];
        }

        $pivotPairs = self::fetchImplicitRelatedPairs(
            $pdo,
            $dbType,
            $relationInfo['table'],
            $relationInfo['currentColumn'],
            $relationInfo['relatedColumn'],
            array_values($parentIds)
        );

        if ($pivotPairs === []) {
            return $counts;
        }

        $parentToRelatedIds = [];
        $allRelatedIds = [];
        foreach ($pivotPairs as $pair) {
            $parentLookupKey = self::countLookupKey($pair['currentId'] ?? null);
            if ($parentLookupKey === null || !isset($recordIndexesByParent[$parentLookupKey])) {
                continue;
            }

            $relatedId = $pair['relatedId'] ?? null;
            $relatedLookupKey = self::countLookupKey($relatedId);
            if ($relatedLookupKey === null) {
                continue;
            }

            $parentToRelatedIds[$parentLookupKey][] = $relatedId;
            $allRelatedIds[$relatedLookupKey] = $relatedId;
        }

        if ($allRelatedIds === []) {
            return $counts;
        }

        $countWhereUsesRelationFilters = $countWhere !== []
            && self::countWhereUsesRelationFilters($countWhere, $relatedInstance->_fields);
        $queryWhere = [$relatedInstance->_primaryKey => ['in' => array_values($allRelatedIds)]];
        if (!$countWhereUsesRelationFilters && $countWhere !== []) {
            $queryWhere = self::mergeWhereClauses($queryWhere, $countWhere);
        }

        $candidateRows = $relatedInstance->findMany(['where' => $queryWhere]);
        if ($candidateRows === []) {
            return $counts;
        }

        $preparedRows = $countWhereUsesRelationFilters
            ? self::prepareCountFilterRecords($candidateRows, $countWhere, $relatedInstance)
            : self::normalizeCountFilterValue($candidateRows);

        $rowsById = [];
        foreach ($preparedRows as $preparedRow) {
            if (!is_array($preparedRow)) {
                continue;
            }

            $lookupKey = self::countLookupKey($preparedRow[$relatedInstance->_primaryKey] ?? null);
            if ($lookupKey === null) {
                continue;
            }

            $rowsById[$lookupKey] = $preparedRow;
        }

        foreach ($parentToRelatedIds as $parentLookupKey => $relatedIds) {
            $count = 0;
            foreach ($relatedIds as $relatedId) {
                $relatedLookupKey = self::countLookupKey($relatedId);
                if ($relatedLookupKey === null || !isset($rowsById[$relatedLookupKey])) {
                    continue;
                }

                if (
                    !$countWhereUsesRelationFilters
                    || self::countRecordMatchesWhere($rowsById[$relatedLookupKey], $countWhere, $relatedInstance)
                ) {
                    $count++;
                }
            }

            foreach ($recordIndexesByParent[$parentLookupKey] as $idx) {
                $counts[$idx] = $count;
            }
        }

        return $counts;
    }

    private static function fetchImplicitRelatedPairs(
        PDO $pdo,
        string $dbType,
        string $tableName,
        string $currentColumn,
        string $relatedColumn,
        array $currentIds
    ): array {
        $currentIds = array_values(array_filter($currentIds, static fn(mixed $value): bool => $value !== null));
        if ($currentIds === []) {
            return [];
        }

        $pivotTableName = self::quoteColumnName($dbType, $tableName);
        $currentColumnQuoted = self::quoteColumnName($dbType, $currentColumn);
        $relatedColumnQuoted = self::quoteColumnName($dbType, $relatedColumn);
        $placeholders = implode(', ', array_fill(0, count($currentIds), '?'));

        $stmt = $pdo->prepare(
            "SELECT {$currentColumnQuoted} AS currentId, {$relatedColumnQuoted} AS relatedId FROM {$pivotTableName} WHERE {$currentColumnQuoted} IN ({$placeholders})"
        );
        $stmt->execute($currentIds);

        return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    }

    private static function countRelatedRecordsThroughModelQuery(
        object $relatedInstance,
        array $baseWhere,
        array $countWhere = []
    ): int {
        $records = $relatedInstance->findMany($baseWhere === [] ? [] : ['where' => $baseWhere]);

        if ($countWhere === []) {
            return count($records);
        }

        $normalizedRecords = self::prepareCountFilterRecords($records, $countWhere, $relatedInstance);

        $count = 0;
        foreach ($normalizedRecords as $normalizedRecord) {
            if (!is_array($normalizedRecord)) {
                continue;
            }

            if (self::countRecordMatchesWhere($normalizedRecord, $countWhere, $relatedInstance)) {
                $count++;
            }
        }

        return $count;
    }

    private static function buildCountFilterIncludesForModel(array $where, object $modelInstance): array
    {
        $includes = [];

        foreach ($where as $key => $value) {
            if (in_array($key, ['AND', 'OR', 'NOT'], true)) {
                if (!is_array($value)) {
                    continue;
                }

                $operands = array_is_list($value) ? $value : [$value];
                foreach ($operands as $operand) {
                    if (!is_array($operand)) {
                        continue;
                    }

                    $includes = self::mergeCountFilterIncludeTrees(
                        $includes,
                        self::buildCountFilterIncludesForModel($operand, $modelInstance)
                    );
                }

                continue;
            }

            $fieldMeta = $modelInstance->_fields[$key] ?? null;
            if (($fieldMeta['kind'] ?? null) !== 'object') {
                continue;
            }

            $relatedInstance = self::makeRelatedInstance($fieldMeta['type'], $modelInstance->_pdo);
            $nestedIncludes = [];

            if (($fieldMeta['isList'] ?? false) === true) {
                if (is_array($value)) {
                    foreach (['some', 'none', 'every'] as $operator) {
                        if (isset($value[$operator]) && is_array($value[$operator])) {
                            $nestedIncludes = self::mergeCountFilterIncludeTrees(
                                $nestedIncludes,
                                self::buildCountFilterIncludesForModel($value[$operator], $relatedInstance)
                            );
                        }
                    }
                }
            } elseif (is_array($value)) {
                $handledNestedOperator = false;
                foreach (['is', 'isNot'] as $operator) {
                    if (!array_key_exists($operator, $value)) {
                        continue;
                    }

                    $handledNestedOperator = true;
                    if (is_array($value[$operator])) {
                        $nestedIncludes = self::mergeCountFilterIncludeTrees(
                            $nestedIncludes,
                            self::buildCountFilterIncludesForModel($value[$operator], $relatedInstance)
                        );
                    }
                }

                if (!$handledNestedOperator) {
                    $nestedIncludes = self::buildCountFilterIncludesForModel($value, $relatedInstance);
                }
            }

            $relationInclude = $nestedIncludes === [] ? true : ['include' => $nestedIncludes];
            if (!isset($includes[$key])) {
                $includes[$key] = $relationInclude;
                continue;
            }

            $includes[$key] = self::mergeCountFilterIncludeOption($includes[$key], $relationInclude);
        }

        return $includes;
    }

    private static function mergeCountFilterIncludeTrees(array $left, array $right): array
    {
        foreach ($right as $relationName => $relationOption) {
            if (!isset($left[$relationName])) {
                $left[$relationName] = $relationOption;
                continue;
            }

            $left[$relationName] = self::mergeCountFilterIncludeOption($left[$relationName], $relationOption);
        }

        return $left;
    }

    private static function mergeCountFilterIncludeOption(mixed $left, mixed $right): mixed
    {
        if ($left === true) {
            return $right;
        }

        if ($right === true) {
            return $left;
        }

        $leftIncludes = is_array($left['include'] ?? null) ? $left['include'] : [];
        $rightIncludes = is_array($right['include'] ?? null) ? $right['include'] : [];

        return [
            'include' => self::mergeCountFilterIncludeTrees($leftIncludes, $rightIncludes),
        ];
    }

    private static function normalizeCountFilterValue(mixed $value): mixed
    {
        if (is_object($value)) {
            if (method_exists($value, 'toArray')) {
                return self::normalizeCountFilterValue($value->toArray());
            }

            return self::normalizeCountFilterValue(get_object_vars($value));
        }

        if (!is_array($value)) {
            return $value;
        }

        $normalized = [];
        foreach ($value as $key => $item) {
            $normalized[$key] = self::normalizeCountFilterValue($item);
        }

        return $normalized;
    }

    private static function countRecordMatchesWhere(array &$record, array $where, object $modelInstance): bool
    {
        foreach ($where as $key => $value) {
            if ($key === 'AND') {
                if (!is_array($value)) {
                    return false;
                }

                $operands = array_is_list($value) ? $value : [$value];
                foreach ($operands as $operand) {
                    if (!is_array($operand) || !self::countRecordMatchesWhere($record, $operand, $modelInstance)) {
                        return false;
                    }
                }

                continue;
            }

            if ($key === 'OR') {
                if (!is_array($value)) {
                    return false;
                }

                $operands = array_is_list($value) ? $value : [$value];
                $matched = false;
                foreach ($operands as $operand) {
                    if (is_array($operand) && self::countRecordMatchesWhere($record, $operand, $modelInstance)) {
                        $matched = true;
                        break;
                    }
                }

                if (!$matched) {
                    return false;
                }

                continue;
            }

            if ($key === 'NOT') {
                if (!is_array($value)) {
                    return false;
                }

                $operands = array_is_list($value) ? $value : [$value];
                foreach ($operands as $operand) {
                    if (is_array($operand) && self::countRecordMatchesWhere($record, $operand, $modelInstance)) {
                        return false;
                    }
                }

                continue;
            }

            $fieldMeta = $modelInstance->_fields[$key] ?? null;
            if (($fieldMeta['kind'] ?? null) === 'object') {
                if (!self::countRecordMatchesRelationFilter($record, $key, $value, $fieldMeta, $modelInstance)) {
                    return false;
                }

                continue;
            }

            if (!self::countRecordMatchesScalarFilter($record[$key] ?? null, $value)) {
                return false;
            }
        }

        return true;
    }

    private static function countRecordMatchesRelationFilter(
        array &$record,
        string $relationName,
        mixed $filter,
        array $fieldMeta,
        object $modelInstance
    ): bool {
        $relatedInstance = self::makeRelatedInstance($fieldMeta['type'], $modelInstance->_pdo);
        $relatedValue = self::loadRelationForCountFilter($record, $relationName, $modelInstance);

        if (($fieldMeta['isList'] ?? false) === true) {
            $relatedRecords = is_array($relatedValue) ? $relatedValue : [];

            if (!is_array($filter)) {
                return false;
            }

            if (array_key_exists('some', $filter)) {
                foreach ($relatedRecords as $relatedRecord) {
                    $normalizedRelatedRecord = self::normalizeCountFilterValue($relatedRecord);
                    if (
                        is_array($normalizedRelatedRecord)
                        && self::countRecordMatchesWhere($normalizedRelatedRecord, $filter['some'], $relatedInstance)
                    ) {
                        return true;
                    }
                }

                return false;
            }

            if (array_key_exists('none', $filter)) {
                foreach ($relatedRecords as $relatedRecord) {
                    $normalizedRelatedRecord = self::normalizeCountFilterValue($relatedRecord);
                    if (
                        is_array($normalizedRelatedRecord)
                        && self::countRecordMatchesWhere($normalizedRelatedRecord, $filter['none'], $relatedInstance)
                    ) {
                        return false;
                    }
                }

                return true;
            }

            if (array_key_exists('every', $filter)) {
                foreach ($relatedRecords as $relatedRecord) {
                    $normalizedRelatedRecord = self::normalizeCountFilterValue($relatedRecord);
                    if (
                        !is_array($normalizedRelatedRecord)
                        || !self::countRecordMatchesWhere($normalizedRelatedRecord, $filter['every'], $relatedInstance)
                    ) {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }

        $relatedRecord = self::normalizeCountFilterValue($relatedValue);
        $hasRelatedRecord = is_array($relatedRecord) && $relatedRecord !== [];

        if ($filter === null) {
            return !$hasRelatedRecord;
        }

        if (!is_array($filter)) {
            return false;
        }

        if (array_key_exists('is', $filter)) {
            $nestedWhere = $filter['is'];

            if ($nestedWhere === null) {
                return !$hasRelatedRecord;
            }

            return $hasRelatedRecord && self::countRecordMatchesWhere($relatedRecord, $nestedWhere, $relatedInstance);
        }

        if (array_key_exists('isNot', $filter)) {
            $nestedWhere = $filter['isNot'];

            if ($nestedWhere === null) {
                return $hasRelatedRecord;
            }

            return !$hasRelatedRecord || !self::countRecordMatchesWhere($relatedRecord, $nestedWhere, $relatedInstance);
        }

        return $hasRelatedRecord && self::countRecordMatchesWhere($relatedRecord, $filter, $relatedInstance);
    }

    private static function loadRelationForCountFilter(array &$record, string $relationName, object $modelInstance): mixed
    {
        if (array_key_exists($relationName, $record)) {
            return $record[$relationName];
        }

        $loadedRecord = self::populateIncludedRelations(
            $record,
            [$relationName => true],
            $modelInstance->_fields,
            $modelInstance->_fieldsRelatedWithKeys,
            $modelInstance->_pdo,
            $modelInstance->_dbType,
            $modelInstance->_modelName,
        );

        $record = self::normalizeCountFilterValue($loadedRecord);

        return $record[$relationName] ?? null;
    }

    private static function countRecordMatchesScalarFilter(mixed $actual, mixed $filter): bool
    {
        if (!is_array($filter) || self::isOperatorArray($filter)) {
            return self::countRecordMatchesScalarOperators($actual, $filter);
        }

        return self::countRecordMatchesScalarOperators($actual, $filter);
    }

    private static function countRecordMatchesScalarOperators(mixed $actual, mixed $filter): bool
    {
        if (!is_array($filter)) {
            return self::countScalarValuesEqual($actual, $filter);
        }

        foreach ($filter as $operator => $expected) {
            switch ($operator) {
                case 'equals':
                    if (!self::countScalarValuesEqual($actual, $expected)) {
                        return false;
                    }
                    break;

                case 'not':
                    if (is_array($expected)) {
                        if (self::countRecordMatchesScalarOperators($actual, $expected)) {
                            return false;
                        }
                    } elseif (self::countScalarValuesEqual($actual, $expected)) {
                        return false;
                    }
                    break;

                case 'contains':
                    if (!is_string($actual) || !is_string($expected) || stripos($actual, $expected) === false) {
                        return false;
                    }
                    break;

                case 'startsWith':
                    if (!is_string($actual) || !is_string($expected) || stripos($actual, $expected) !== 0) {
                        return false;
                    }
                    break;

                case 'endsWith':
                    if (!is_string($actual) || !is_string($expected)) {
                        return false;
                    }

                    $actualLength = strlen($actual);
                    $expectedLength = strlen($expected);
                    if ($expectedLength > $actualLength || strcasecmp(substr($actual, -$expectedLength), $expected) !== 0) {
                        return false;
                    }
                    break;

                case 'gt':
                case 'gte':
                case 'lt':
                case 'lte':
                    if (!self::countCompareScalarValues($actual, $expected, $operator)) {
                        return false;
                    }
                    break;

                case 'in':
                    if (!is_array($expected) || !self::countValueInList($actual, $expected)) {
                        return false;
                    }
                    break;

                case 'notIn':
                    if (!is_array($expected) || self::countValueInList($actual, $expected)) {
                        return false;
                    }
                    break;

                default:
                    return false;
            }
        }

        return true;
    }

    private static function countScalarValuesEqual(mixed $actual, mixed $expected): bool
    {
        if ($actual instanceof \DateTimeInterface) {
            $actual = $actual->format('Y-m-d H:i:s');
        }

        if ($expected instanceof \DateTimeInterface) {
            $expected = $expected->format('Y-m-d H:i:s');
        }

        if ($actual === null || $expected === null) {
            return $actual === $expected;
        }

        if (is_bool($actual) || is_bool($expected)) {
            return (bool)$actual === (bool)$expected;
        }

        if (is_numeric($actual) && is_numeric($expected)) {
            return (string)+$actual === (string)+$expected;
        }

        return (string)$actual === (string)$expected;
    }

    private static function countValueInList(mixed $actual, array $expectedList): bool
    {
        foreach ($expectedList as $expected) {
            if (self::countScalarValuesEqual($actual, $expected)) {
                return true;
            }
        }

        return false;
    }

    private static function countCompareScalarValues(mixed $actual, mixed $expected, string $operator): bool
    {
        if ($actual === null || $expected === null) {
            return false;
        }

        if (is_numeric($actual) && is_numeric($expected)) {
            $left = +$actual;
            $right = +$expected;
        } elseif (($actualTime = strtotime((string)$actual)) !== false && ($expectedTime = strtotime((string)$expected)) !== false) {
            $left = $actualTime;
            $right = $expectedTime;
        } else {
            $left = (string)$actual;
            $right = (string)$expected;
        }

        return match ($operator) {
            'gt' => $left > $right,
            'gte' => $left >= $right,
            'lt' => $left < $right,
            'lte' => $left <= $right,
        };
    }

    private static function fetchImplicitRelatedRecordIds(
        PDO $pdo,
        string $dbType,
        string $tableName,
        string $currentColumn,
        string $relatedColumn,
        mixed $currentId
    ): array {
        $pivotTableName = self::quoteColumnName($dbType, $tableName);
        $currentColumnQuoted = self::quoteColumnName($dbType, $currentColumn);
        $relatedColumnQuoted = self::quoteColumnName($dbType, $relatedColumn);

        $stmt = $pdo->prepare(
            "SELECT {$relatedColumnQuoted} FROM {$pivotTableName} WHERE {$currentColumnQuoted} = :implicit_current_id"
        );
        self::bindValues($stmt, [':implicit_current_id' => $currentId]);
        $stmt->execute();

        return array_values(array_filter(
            $stmt->fetchAll(PDO::FETCH_COLUMN),
            static fn(mixed $value): bool => $value !== null
        ));
    }

    private static function resolveImplicitCountRecordId(array $record): mixed
    {
        if (array_key_exists('id', $record) && $record['id'] !== null) {
            return $record['id'];
        }

        foreach ($record as $key => $value) {
            if ($value !== null && str_ends_with($key, 'Id')) {
                return $value;
            }
        }

        return null;
    }

    private static function countRelatedRecords(
        array $record,
        string $relationName,
        array $relatedField,
        array $relatedKeys,
        PDO $pdo,
        string $dbType,
        array $countWhere = [],
        string $currentModelName = ''
    ): int {
        $relatedInstance = self::makeRelatedInstance($relatedField['type'], $pdo);
        $relatedTableName = self::quoteColumnName($dbType, $relatedInstance->_tableName);
        $countWhereUsesRelationFilters = $countWhere !== []
            && self::countWhereUsesRelationFilters($countWhere, $relatedInstance->_fields);
        $isImplicitSelfRelation = $currentModelName !== ''
            && $currentModelName === $relatedInstance->_modelName;

        if (
            empty($relatedKeys['relationFromFields']) && empty($relatedKeys['relationToFields'])
            && $isImplicitSelfRelation
        ) {
            $selfRelationName = $relatedField['relationName'] ?? null;

            foreach ($relatedInstance->_fields as $fieldName => $fieldMeta) {
                if (($fieldMeta['relationName'] ?? null) === $selfRelationName
                    && !empty($relatedInstance->_fieldsRelatedWithKeys[$fieldName]['relationFromFields'])
                ) {
                    $oppositeKeys = $relatedInstance->_fieldsRelatedWithKeys[$fieldName];
                    $relatedKeys = [
                        'relationFromFields' => $oppositeKeys['relationFromFields'],
                        'relationToFields' => $oppositeKeys['relationToFields'],
                    ];
                    break;
                }
            }
        }

        if (!empty($relatedKeys['relationFromFields']) && !empty($relatedKeys['relationToFields'])) {
            $baseWhere = [];
            foreach ($relatedKeys['relationFromFields'] as $i => $fromField) {
                $toField = $relatedKeys['relationToFields'][$i];

                if (isset($relatedInstance->_fields[$fromField])) {
                    $baseWhere[$fromField] = $record[$toField] ?? null;
                } else {
                    $baseWhere[$toField] = $record[$fromField] ?? null;
                }
            }

            if (empty(array_filter($baseWhere, static fn(mixed $value): bool => $value !== null))) {
                return 0;
            }

            if ($countWhereUsesRelationFilters) {
                return self::countRelatedRecordsThroughModelQuery($relatedInstance, $baseWhere, $countWhere);
            }

            $whereClause = [];
            $bindings = [];
            $counter = 0;

            foreach ($baseWhere as $field => $value) {
                $placeholder = ':count_' . $counter++;
                $quotedField = self::quoteColumnName($dbType, $field);
                $whereClause[] = "$quotedField = $placeholder";
                $bindings[$placeholder] = $value;
            }

            if ($countWhere !== []) {
                self::processConditions($countWhere, $whereClause, $bindings, $dbType, $relatedTableName, 'related_count_');
            }

            $sql = "SELECT COUNT(*) FROM $relatedTableName WHERE " . implode(' AND ', $whereClause);

            $stmt = $pdo->prepare($sql);
            self::bindValues($stmt, $bindings);
            $stmt->execute();

            return (int) $stmt->fetchColumn();
        }

        if (empty($relatedKeys['relationFromFields']) && empty($relatedKeys['relationToFields'])) {
            if ($isImplicitSelfRelation) {
                $idField = self::detectIdField($record, $relatedField, $relatedInstance);
                $idValue = $record[$idField] ?? null;

                if ($idValue === null) {
                    return 0;
                }

                $relationInfo = self::resolveImplicitRelationTable(
                    $relatedField,
                    $relatedInstance,
                    $relatedInstance->_modelName
                );

                if ($countWhereUsesRelationFilters) {
                    $relatedIds = self::fetchImplicitRelatedRecordIds(
                        $pdo,
                        $dbType,
                        $relationInfo['table'],
                        $relationInfo['currentColumn'],
                        $relationInfo['relatedColumn'],
                        $idValue
                    );

                    if ($relatedIds === []) {
                        return 0;
                    }

                    return self::countRelatedRecordsThroughModelQuery(
                        $relatedInstance,
                        [$relatedInstance->_primaryKey => ['in' => $relatedIds]],
                        $countWhere
                    );
                }

                $tableName = self::quoteColumnName($dbType, $relationInfo['table']);
                $searchColumnQuoted = self::quoteColumnName($dbType, $relationInfo['currentColumn']);
                $relatedColumnQuoted = self::quoteColumnName($dbType, $relationInfo['relatedColumn']);

                $whereClauses = ["$tableName.$searchColumnQuoted = :id"];
                $bindings = [':id' => $idValue];
                $sql = "SELECT COUNT(*) FROM $tableName";

                if ($countWhere !== []) {
                    $relatedPrimaryKey = self::quoteColumnName($dbType, $relatedInstance->_primaryKey);
                    $sql .= " INNER JOIN $relatedTableName ON $tableName.$relatedColumnQuoted = $relatedTableName.$relatedPrimaryKey";
                    self::processConditions($countWhere, $whereClauses, $bindings, $dbType, $relatedTableName, 'related_count_');
                }

                $sql .= ' WHERE ' . implode(' AND ', $whereClauses);
                $stmt = $pdo->prepare($sql);
                self::bindValues($stmt, $bindings);
                $stmt->execute();

                return (int) $stmt->fetchColumn();
            }

            if ($currentModelName === '') {
                return 0;
            }

            $implicitModelInfo = self::compareStringsAlphabetically($currentModelName, $relatedField['type']);
            $currentColumn = $currentModelName === $implicitModelInfo['A'] ? 'A' : 'B';
            $relatedColumn = $currentColumn === 'A' ? 'B' : 'A';
            $idValue = self::resolveImplicitCountRecordId($record);

            if ($idValue === null) {
                return 0;
            }

            if ($countWhereUsesRelationFilters) {
                $relatedIds = self::fetchImplicitRelatedRecordIds(
                    $pdo,
                    $dbType,
                    $implicitModelInfo['Name'],
                    $currentColumn,
                    $relatedColumn,
                    $idValue
                );

                if ($relatedIds === []) {
                    return 0;
                }

                return self::countRelatedRecordsThroughModelQuery(
                    $relatedInstance,
                    [$relatedInstance->_primaryKey => ['in' => $relatedIds]],
                    $countWhere
                );
            }

            $tableName = self::quoteColumnName($dbType, $implicitModelInfo['Name']);
            $currentColumnQuoted = self::quoteColumnName($dbType, $currentColumn);
            $relatedColumnQuoted = self::quoteColumnName($dbType, $relatedColumn);
            $whereClauses = ["$tableName.$currentColumnQuoted = :id"];
            $bindings = [':id' => $idValue];
            $sql = "SELECT COUNT(*) FROM $tableName";

            if ($countWhere !== []) {
                $relatedPrimaryKey = self::quoteColumnName($dbType, $relatedInstance->_primaryKey);
                $sql .= " INNER JOIN $relatedTableName ON $tableName.$relatedColumnQuoted = $relatedTableName.$relatedPrimaryKey";
                self::processConditions($countWhere, $whereClauses, $bindings, $dbType, $relatedTableName, 'related_count_');
            }

            $sql .= ' WHERE ' . implode(' AND ', $whereClauses);
            $stmt = $pdo->prepare($sql);
            self::bindValues($stmt, $bindings);
            $stmt->execute();

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

    public static function inferRelationKeys(array $relatedFieldKeys, array $field, object $relatedInstance): array
    {
        if (!empty($relatedFieldKeys['relationFromFields']) && !empty($relatedFieldKeys['relationToFields'])) {
            return $relatedFieldKeys;
        }

        $relationName = $field['relationName'] ?? null;
        $fieldName = $field['name'] ?? null;
        if ($relationName === null) {
            return $relatedFieldKeys;
        }

        foreach ($relatedInstance->_fields as $candidateField) {
            if (($candidateField['relationName'] ?? null) !== $relationName) {
                continue;
            }

            if (($candidateField['name'] ?? null) === $fieldName) {
                continue;
            }

            $candidateKeys = $relatedInstance->_fieldsRelatedWithKeys[$candidateField['name']] ?? null;
            if (!empty($candidateKeys['relationFromFields']) && !empty($candidateKeys['relationToFields'])) {
                return $candidateKeys;
            }
        }

        return $relatedFieldKeys;
    }

    public static function resolveImplicitRelationTable(array $field, object $relatedInstance, string $currentModelName): array
    {
        $relatedModel = $field['type'] ?? null;
        if ($relatedModel === null) {
            throw new Exception('Implicit relation metadata is missing the related model type.');
        }

        if ($relatedModel === $currentModelName) {
            $relationName = $field['relationName'] ?? null;
            if ($relationName === null) {
                throw new Exception('Implicit self relation is missing a relation name.');
            }

            $sameRelationFields = array_values(array_filter(
                $relatedInstance->_fields,
                static fn($candidate) => ($candidate['relationName'] ?? null) === $relationName
                    && ($candidate['kind'] ?? null) === 'object'
                    && ($candidate['isList'] ?? false)
            ));

            usort(
                $sameRelationFields,
                static fn($left, $right) => strcmp($left['name'] ?? '', $right['name'] ?? '')
            );

            $firstFieldName = $sameRelationFields[0]['name'] ?? ($field['name'] ?? '');
            $currentColumn = ($field['name'] ?? null) === $firstFieldName ? 'A' : 'B';

            return [
                'table' => '_' . $relationName,
                'currentColumn' => $currentColumn,
                'relatedColumn' => $currentColumn === 'A' ? 'B' : 'A',
            ];
        }

        $implicitModelInfo = self::compareStringsAlphabetically($currentModelName, $relatedModel);
        $currentColumn = $currentModelName === $implicitModelInfo['A'] ? 'A' : 'B';

        return [
            'table' => $implicitModelInfo['Name'],
            'currentColumn' => $currentColumn,
            'relatedColumn' => $currentColumn === 'A' ? 'B' : 'A',
        ];
    }

    private static function isImplicitManyToMany(array $relatedKeys, array $instanceField): bool
    {
        return empty($relatedKeys['relationFromFields'])
            && empty($relatedKeys['relationToFields'])
            && empty($instanceField['relationFromFields'])
            && empty($instanceField['relationToFields']);
    }

    private static function resolveToOneBatchLookup(array $relatedField, array $relatedInstanceField): ?array
    {
        $directFromFields = $relatedField['relationFromFields'] ?? [];
        $directToFields = $relatedField['relationToFields'] ?? [];
        if (count($directFromFields) === 1 && count($directToFields) === 1) {
            return [
                'parentField' => $directFromFields[0],
                'lookupField' => $directToFields[0],
            ];
        }

        $inverseFromFields = $relatedInstanceField['relationFromFields'] ?? [];
        $inverseToFields = $relatedInstanceField['relationToFields'] ?? [];
        if (count($inverseFromFields) === 1 && count($inverseToFields) === 1) {
            return [
                'parentField' => $inverseToFields[0],
                'lookupField' => $inverseFromFields[0],
            ];
        }

        return null;
    }

    private static function applyBatchLookupConstraint(array &$baseQuery, string $lookupField, array $lookupValues): void
    {
        $lookupValues = array_values(array_unique(array_filter(
            $lookupValues,
            static fn(mixed $value): bool => $value !== null
        )));

        if ($lookupValues === []) {
            return;
        }

        $lookupWhere = [$lookupField => ['in' => $lookupValues]];
        if (!isset($baseQuery['where']) || $baseQuery['where'] === []) {
            $baseQuery['where'] = $lookupWhere;

            return;
        }

        $baseQuery['where'] = self::mergeWhereClauses($baseQuery['where'], $lookupWhere);
    }

    private static function loadOneToManyBatch(
        object $relatedInstance,
        string $childFk,
        array  $parentIds,
        array  $baseQuery,
    ): array {
        self::applyBatchLookupConstraint($baseQuery, $childFk, $parentIds);

        if (isset($baseQuery['select']) && $baseQuery['select'] !== []) {
            $baseQuery['select'][$childFk] = true;
        }

        if (isset($baseQuery['omit'][$childFk])) {
            unset($baseQuery['omit'][$childFk]);
        }

        $rows = $relatedInstance->findMany($baseQuery);

        $grouped = [];
        foreach ($rows as $row) {
            $key = $row->{$childFk};
            $grouped[$key][] = $row;
        }

        return $grouped;
    }

    private static function loadOneToOneBatch(
        object $relatedInstance,
        string $lookupField,
        array  $lookupValues,
        array  $baseQuery,
    ): array {
        self::applyBatchLookupConstraint($baseQuery, $lookupField, $lookupValues);

        if (isset($baseQuery['select']) && $baseQuery['select'] !== []) {
            $baseQuery['select'][$lookupField] = true;
        }

        if (isset($baseQuery['omit'][$lookupField])) {
            unset($baseQuery['omit'][$lookupField]);
        }

        $rows = $relatedInstance->findMany($baseQuery);

        $grouped = [];
        foreach ($rows as $row) {
            $key = $row->{$lookupField} ?? null;
            if ($key === null) {
                continue;
            }

            $grouped[$key] = $row;
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
        $relationInfo = self::resolveImplicitRelationTable(
            $relatedField,
            $relatedInstance,
            $relatedInstanceField['type']
        );
        $searchColumn = $relationInfo['currentColumn'];
        $returnColumn = $relationInfo['relatedColumn'];
        $idField      = self::detectIdField($singleRecord, $relatedField, $relatedInstance);
        $idValue      = $singleRecord[$idField] ?? null;

        if ($idValue === null) {
            return [];
        }

        $table   = PPHPUtility::quoteColumnName($dbType, $relationInfo['table']);
        $search  = PPHPUtility::quoteColumnName($dbType, $searchColumn);
        $return  = PPHPUtility::quoteColumnName($dbType, $returnColumn);
        $sql     = "SELECT {$return} FROM {$table} WHERE {$search} = :id";
        $stmt    = $pdo->prepare($sql);
        $stmt->execute(['id' => $idValue]);
        $ids     = $stmt->fetchAll(PDO::FETCH_COLUMN);

        if (!$ids) {
            return [];
        }

        $baseQuery['where'] = self::mergeWhereClauses(
            $whereConditions,
            [$relatedInstance->_primaryKey => ['in' => $ids]]
        );

        return $relatedInstance->findMany($baseQuery);
    }

    private static function loadImplicitManyBatch(
        object $relatedInstance,
        array  $relatedField,
        array  $relatedInstanceField,
        array  $records,
        array  $baseQuery,
        array  $whereConditions,
        string $currentModelName,
        string $dbType,
        PDO    $pdo,
    ): ?array {
        if (count($records) < 2) {
            return null;
        }

        $parentIds = [];
        $recordIndexesByParent = [];
        foreach ($records as $recordIndex => $record) {
            $parentId = self::resolveImplicitCountRecordId($record);
            $lookupKey = self::countLookupKey($parentId);
            if ($lookupKey === null) {
                continue;
            }

            $parentIds[$lookupKey] = $parentId;
            $recordIndexesByParent[$lookupKey][] = $recordIndex;
        }

        if ($parentIds === []) {
            return array_fill_keys(array_keys($records), []);
        }

        $isImplicitSelfRelation = $currentModelName !== ''
            && $currentModelName === $relatedInstance->_modelName;

        if ($isImplicitSelfRelation) {
            $relationInfo = self::resolveImplicitRelationTable(
                $relatedField,
                $relatedInstance,
                $relatedInstance->_modelName
            );
        } else {
            if ($currentModelName === '') {
                return null;
            }

            $implicitModelInfo = self::compareStringsAlphabetically($currentModelName, $relatedField['type']);
            $currentColumn = $currentModelName === $implicitModelInfo['A'] ? 'A' : 'B';
            $relationInfo = [
                'table' => $implicitModelInfo['Name'],
                'currentColumn' => $currentColumn,
                'relatedColumn' => $currentColumn === 'A' ? 'B' : 'A',
            ];
        }

        $pivotPairs = self::fetchImplicitRelatedPairs(
            $pdo,
            $dbType,
            $relationInfo['table'],
            $relationInfo['currentColumn'],
            $relationInfo['relatedColumn'],
            array_values($parentIds)
        );

        if ($pivotPairs === []) {
            return array_fill_keys(array_keys($records), []);
        }

        $parentToRelatedIds = [];
        $allRelatedIds = [];
        foreach ($pivotPairs as $pair) {
            $parentLookupKey = self::countLookupKey($pair['currentId'] ?? null);
            if ($parentLookupKey === null || !isset($recordIndexesByParent[$parentLookupKey])) {
                continue;
            }

            $relatedId = $pair['relatedId'] ?? null;
            $relatedLookupKey = self::countLookupKey($relatedId);
            if ($relatedLookupKey === null) {
                continue;
            }

            $parentToRelatedIds[$parentLookupKey][] = $relatedId;
            $allRelatedIds[$relatedLookupKey] = $relatedId;
        }

        if ($allRelatedIds === []) {
            return array_fill_keys(array_keys($records), []);
        }

        $relatedPrimaryKey = $relatedInstance->_primaryKey;
        self::applyBatchLookupConstraint($baseQuery, $relatedPrimaryKey, array_values($allRelatedIds));

        if (isset($baseQuery['select']) && $baseQuery['select'] !== []) {
            $baseQuery['select'][$relatedPrimaryKey] = true;
        }

        if (isset($baseQuery['omit'][$relatedPrimaryKey])) {
            unset($baseQuery['omit'][$relatedPrimaryKey]);
        }

        if ($whereConditions !== [] && (!isset($baseQuery['where']) || $baseQuery['where'] === [])) {
            $baseQuery['where'] = $whereConditions;
            self::applyBatchLookupConstraint($baseQuery, $relatedPrimaryKey, array_values($allRelatedIds));
        }

        $rows = $relatedInstance->findMany($baseQuery);
        if ($rows === []) {
            return array_fill_keys(array_keys($records), []);
        }

        $rowsById = [];
        foreach ($rows as $row) {
            $rowId = $row->{$relatedPrimaryKey} ?? null;
            $rowLookupKey = self::countLookupKey($rowId);
            if ($rowLookupKey === null) {
                continue;
            }

            $rowsById[$rowLookupKey] = $row;
        }

        $grouped = array_fill_keys(array_keys($records), []);
        foreach ($parentToRelatedIds as $parentLookupKey => $relatedIds) {
            $parentRows = [];
            foreach ($relatedIds as $relatedId) {
                $relatedLookupKey = self::countLookupKey($relatedId);
                if ($relatedLookupKey === null || !isset($rowsById[$relatedLookupKey])) {
                    continue;
                }

                $parentRows[] = $rowsById[$relatedLookupKey];
            }

            foreach ($recordIndexesByParent[$parentLookupKey] as $recordIndex) {
                $grouped[$recordIndex] = $parentRows;
            }
        }

        return $grouped;
    }

    private static function makeRelatedInstance(string $model, PDO $pdo): object
    {
        $fqcn = "Lib\\Prisma\\Classes\\{$model}";
        if (!class_exists($fqcn)) {
            throw new Exception("Class {$fqcn} does not exist.");
        }

        $pdoKey = spl_object_id($pdo);
        if (isset(self::$relatedInstanceCache[$pdoKey][$fqcn])) {
            return self::$relatedInstanceCache[$pdoKey][$fqcn];
        }

        self::$relatedInstanceCache[$pdoKey][$fqcn] = new $fqcn($pdo);

        return self::$relatedInstanceCache[$pdoKey][$fqcn];
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
        if (!empty($relatedField['relationFromFields']) && !empty($relatedField['relationToFields'])) {
            foreach ($relatedField['relationFromFields'] as $i => $fromField) {
                $toField = $relatedField['relationToFields'][$i];
                if (array_key_exists($fromField, $singleRecord)) {
                    $where[$toField] = $singleRecord[$fromField];
                }
            }
        } elseif (!empty($relatedInstanceField['relationFromFields']) && !empty($relatedInstanceField['relationToFields'])) {
            foreach ($relatedInstanceField['relationFromFields'] as $i => $fromField) {
                $toField = $relatedInstanceField['relationToFields'][$i];
                if (array_key_exists($toField, $singleRecord)) {
                    $where[$fromField] = $singleRecord[$toField];
                }
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

    private static function unwrapExplicitOneToManyPayload(array $operation): array
    {
        return isset($operation['__related']) && is_array($operation['__related'])
            ? $operation['__related']
            : $operation;
    }

    private static function stripInternalRelationMarkers(array $payload): array
    {
        unset($payload['__relationBoolean']);

        return $payload;
    }

    private static function resolveExplicitParentReference(
        array $operation,
        array $childFkFields,
        array $parentKeyFields,
        string $relatedFieldName
    ): array {
        $payload = self::unwrapExplicitOneToManyPayload($operation);
        $parentData = $operation['__parent'] ?? null;
        $parentReference = [];

        foreach ($childFkFields as $index => $childFkField) {
            if (array_key_exists($childFkField, $operation)) {
                $parentReference[$childFkField] = $operation[$childFkField];
                continue;
            }

            if (array_key_exists($childFkField, $payload)) {
                $parentReference[$childFkField] = $payload[$childFkField];
                continue;
            }

            $parentKeyField = $parentKeyFields[$index] ?? null;
            if (is_array($parentData) && $parentKeyField !== null && array_key_exists($parentKeyField, $parentData)) {
                $parentReference[$childFkField] = $parentData[$parentKeyField];
                continue;
            }

            throw new Exception("Missing parent id in relation payload for '{$relatedFieldName}'.");
        }

        return $parentReference;
    }

    private static function resolveRecordSelector(object $relatedClass, array $payload): array
    {
        $primaryKey = $relatedClass->_primaryKey;
        if (is_string($primaryKey) && $primaryKey !== '' && array_key_exists($primaryKey, $payload)) {
            return [$primaryKey => $payload[$primaryKey]];
        }

        $compositeKeys = $relatedClass->_compositeKeys;
        if (is_array($compositeKeys) && $compositeKeys !== []) {
            $selector = [];
            foreach ($compositeKeys as $key) {
                if (!array_key_exists($key, $payload)) {
                    $selector = [];
                    break;
                }
                $selector[$key] = $payload[$key];
            }
            if ($selector !== []) {
                return $selector;
            }
        }

        $uniqueFields = $relatedClass->_model['uniqueFields'] ?? [];
        if (is_array($uniqueFields)) {
            foreach ($uniqueFields as $uniqueFieldSet) {
                if (!is_array($uniqueFieldSet) || $uniqueFieldSet === []) {
                    continue;
                }

                $selector = [];
                foreach ($uniqueFieldSet as $fieldName) {
                    if (!array_key_exists($fieldName, $payload)) {
                        $selector = [];
                        break;
                    }

                    $selector[$fieldName] = $payload[$fieldName];
                }

                if ($selector !== []) {
                    return $selector;
                }
            }
        }

        foreach ($relatedClass->_fields as $fieldName => $fieldMeta) {
            if (($fieldMeta['kind'] ?? 'scalar') !== 'scalar') {
                continue;
            }

            if (($fieldMeta['isUnique'] ?? false) && array_key_exists($fieldName, $payload)) {
                return [$fieldName => $payload[$fieldName]];
            }
        }

        return [];
    }

    private static function resolveOperationWhere(
        object $relatedClass,
        array $payload,
        string $relatedFieldName,
        string $action
    ): array {
        if (isset($payload['where']) && is_array($payload['where'])) {
            return $payload['where'];
        }

        $where = self::resolveRecordSelector($relatedClass, $payload);
        if ($where === []) {
            throw new Exception("{$action} requires a unique selector for '{$relatedFieldName}'.");
        }

        return $where;
    }

    private static function tryFastExplicitOneToManyUpsert(
        object $relatedClass,
        PDO $pdo,
        string $dbType,
        array $where,
        array $updateData
    ): bool {
        if (!self::canUseDirectModelMutationData($relatedClass, $updateData)) {
            return false;
        }

        $existing = $relatedClass->findUnique(['where' => $where]);
        if ($existing === null) {
            return false;
        }

        self::executeDirectModelUpdate($relatedClass, $pdo, $dbType, $where, $updateData);

        return true;
    }

    private static function tryFastExplicitOneToManyDisconnect(
        object $relatedClass,
        PDO $pdo,
        string $dbType,
        array $childFkFields,
        array $operations
    ): bool {
        if ($childFkFields === []) {
            return false;
        }

        $detachData = array_fill_keys($childFkFields, null);

        foreach ($operations as $operation) {
            $payload = self::unwrapExplicitOneToManyPayload($operation);
            $where = array_diff_key($payload, array_flip($childFkFields));
            if ($where === []) {
                $where = self::resolveRecordSelector($relatedClass, $payload);
            }

            if ($where === []) {
                return false;
            }

            self::executeDirectModelUpdate($relatedClass, $pdo, $dbType, $where, $detachData);
        }

        return true;
    }

    private static function tryFastExplicitOneToManySet(
        object $relatedClass,
        PDO $pdo,
        string $dbType,
        array $childFkFields,
        array $parentKeyFields,
        string $relatedFieldName,
        array $operations
    ): bool {
        if ($operations === [] || $childFkFields === []) {
            return false;
        }

        $primaryKey = $relatedClass->_primaryKey;
        if (!is_string($primaryKey) || $primaryKey === '') {
            return false;
        }

        $parentReference = self::resolveExplicitParentReference(
            $operations[0],
            $childFkFields,
            $parentKeyFields,
            $relatedFieldName
        );

        $selectorMap = [];
        $selectFields = [$primaryKey => true];
        foreach ($childFkFields as $childFkField) {
            $selectFields[$childFkField] = true;
        }

        foreach ($operations as $operation) {
            $payload = self::unwrapExplicitOneToManyPayload($operation);
            $selector = self::resolveRecordSelector($relatedClass, $payload);
            if ($selector === []) {
                return false;
            }

            $payloadWithoutChildKeys = array_diff_key($payload, array_flip($childFkFields));
            if (array_diff_key($payloadWithoutChildKeys, $selector) !== []) {
                return false;
            }

            foreach (array_keys($selector) as $fieldName) {
                $selectFields[$fieldName] = true;
            }

            $selectorMap[self::selectorSignature($selector)] = $selector;
        }

        $records = $relatedClass->findMany([
            'where' => ['OR' => array_values($selectorMap)],
            'select' => $selectFields,
        ]);

        if (count($records) !== count($selectorMap)) {
            return false;
        }

        $recordMap = [];
        foreach ($records as $record) {
            $recordArray = (array) $record;

            foreach ($selectorMap as $signature => $selector) {
                if (self::selectorSignature(array_intersect_key($recordArray, $selector)) === $signature) {
                    $recordMap[$signature] = $recordArray;
                    break;
                }
            }
        }

        if (count($recordMap) !== count($selectorMap)) {
            return false;
        }

        $attachedIds = [];
        foreach ($recordMap as $record) {
            $attachedIds[] = $record[$primaryKey];
        }
        $attachedIds = array_values(array_unique($attachedIds));

        if ($attachedIds !== []) {
            self::executeDirectModelUpdate(
                $relatedClass,
                $pdo,
                $dbType,
                [$primaryKey => ['in' => $attachedIds]],
                $parentReference
            );
        }

        $childFieldMeta = $relatedClass->_fields[$childFkFields[0]] ?? [];
        $detachWhere = $parentReference;
        if ($attachedIds !== []) {
            $detachWhere[$primaryKey] = ['notIn' => $attachedIds];
        }

        if (($childFieldMeta['isRequired'] ?? false) === false) {
            self::executeDirectModelUpdate(
                $relatedClass,
                $pdo,
                $dbType,
                $detachWhere,
                array_fill_keys($childFkFields, null)
            );
        } else {
            self::executeDirectModelDelete($relatedClass, $pdo, $dbType, $detachWhere);
        }

        return true;
    }

    private static function executeDirectModelUpdate(
        object $model,
        PDO $pdo,
        string $dbType,
        array $where,
        array $data
    ): int {
        if ($data === []) {
            return 0;
        }

        $tableName = self::quoteColumnName($dbType, $model->_tableName);
        $setClauses = [];
        $bindings = [];
        $bindingIndex = 0;

        foreach ($data as $fieldName => $value) {
            $fieldMeta = $model->_fields[$fieldName] ?? null;
            if (!is_array($fieldMeta) || ($fieldMeta['kind'] ?? 'scalar') === 'object') {
                throw new Exception("Cannot fast-update non-scalar field '{$fieldName}' on {$model->_modelName}.");
            }

            $columnName = self::quoteColumnName($dbType, $fieldMeta['dbName'] ?? $fieldName);
            $placeholder = ':fast_set_' . $bindingIndex++;
            $setClauses[] = "$columnName = $placeholder";
            $bindings[$placeholder] = self::normalizeDirectMutationValue($fieldMeta, $value);
        }

        foreach ($model->_fields as $fieldName => $fieldMeta) {
            if (($fieldMeta['isUpdatedAt'] ?? false) !== true || array_key_exists($fieldName, $data)) {
                continue;
            }

            $columnName = self::quoteColumnName($dbType, $fieldMeta['dbName'] ?? $fieldName);
            $placeholder = ':fast_set_' . $bindingIndex++;
            $setClauses[] = "$columnName = $placeholder";
            $bindings[$placeholder] = date('Y-m-d H:i:s');
        }

        $conditions = [];
        self::processConditions($where, $conditions, $bindings, $dbType, $tableName, 'fast_where_', 0, $model->_fields);

        if ($conditions === []) {
            return 0;
        }

        $sql = 'UPDATE ' . $tableName . ' SET ' . implode(', ', $setClauses) . ' WHERE ' . implode(' AND ', $conditions);
        $stmt = $pdo->prepare($sql);
        self::bindValues($stmt, $bindings);
        $stmt->execute();

        return $stmt->rowCount();
    }

    private static function executeDirectModelDelete(object $model, PDO $pdo, string $dbType, array $where): int
    {
        $tableName = self::quoteColumnName($dbType, $model->_tableName);
        $bindings = [];
        $conditions = [];
        self::processConditions($where, $conditions, $bindings, $dbType, $tableName, 'fast_delete_', 0, $model->_fields);

        if ($conditions === []) {
            return 0;
        }

        $sql = 'DELETE FROM ' . $tableName . ' WHERE ' . implode(' AND ', $conditions);
        $stmt = $pdo->prepare($sql);
        self::bindValues($stmt, $bindings);
        $stmt->execute();

        return $stmt->rowCount();
    }

    private static function canUseDirectModelMutationData(object $model, array $data): bool
    {
        foreach ($data as $fieldName => $value) {
            $fieldMeta = $model->_fields[$fieldName] ?? null;
            if (!is_array($fieldMeta)) {
                return false;
            }

            if (($fieldMeta['kind'] ?? 'scalar') === 'object') {
                return false;
            }

            if ($value instanceof ModelFieldReference) {
                return false;
            }

            if (is_array($value) && !array_is_list($value)) {
                return false;
            }
        }

        return true;
    }

    private static function normalizeDirectMutationValue(array $fieldMeta, mixed $value): mixed
    {
        if ($value instanceof UnitEnum) {
            $value = $value->value;
        }

        if ($value === null) {
            return null;
        }

        $kind = $fieldMeta['kind'] ?? 'scalar';
        $type = $fieldMeta['type'] ?? null;
        $isList = (bool) ($fieldMeta['isList'] ?? false);

        if ($kind === 'enum') {
            $enumClass = __NAMESPACE__ . '\\' . $type;
            $validated = Validator::enumClass($value, $enumClass);
            if ($validated === null) {
                throw new InvalidArgumentException("Invalid enum value for field '{$fieldMeta['name']}'.");
            }

            return $isList ? json_encode($validated) : $validated;
        }

        $validateMethodName = is_string($type) ? lcfirst($type) : 'string';
        if (!method_exists(Validator::class, $validateMethodName)) {
            return Validator::string($value, false);
        }

        $validated = $type === 'Decimal'
            ? Validator::$validateMethodName($value, !empty($fieldMeta['nativeType'][1]) ? intval($fieldMeta['nativeType'][1][1]) : 30)
            : Validator::$validateMethodName($value);

        if ($type === 'Boolean') {
            return $validated ? 1 : 0;
        }

        if (is_object($validated) && method_exists($validated, '__toString')) {
            return $validated->__toString();
        }

        return $validated;
    }

    private static function selectorSignature(array $selector): string
    {
        ksort($selector);

        return json_encode($selector, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
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
            'in'           => 'IN',
            'notIn'        => 'NOT IN',
            'between'      => 'BETWEEN',
            default        => throw new Exception("Unsupported operator '$op' in HAVING.")
        };
    }

    private static function isGroupByHavingLogicalOperator(string $key): bool
    {
        return in_array($key, ['AND', 'OR', 'NOT'], true);
    }

    private static function isGroupByHavingScalarOperator(string $key): bool
    {
        return in_array($key, ['equals', 'not', 'gt', 'gte', 'lt', 'lte', 'in', 'notIn', 'between'], true);
    }

    public static function validateGroupByHaving(
        array $having,
        array $by,
        array $fields,
        string $modelName
    ): void {
        if ($having === []) {
            return;
        }

        $missingFields = [];
        self::collectGroupByHavingValidationIssues($having, $by, $fields, $modelName, $missingFields);

        if ($missingFields !== []) {
            throw new Exception(
                'Every field used in `having` filters must either be an aggregation filter or be included in the selection of the query. Missing fields: '
                    . implode(', ', array_values(array_unique($missingFields)))
            );
        }
    }

    private static function collectGroupByHavingValidationIssues(
        array $having,
        array $by,
        array $fields,
        string $modelName,
        array &$missingFields
    ): void {
        $aggregateKeys = ['_count', '_avg', '_sum', '_min', '_max'];

        foreach ($having as $key => $value) {
            if (self::isGroupByHavingLogicalOperator($key)) {
                if (!is_array($value) || $value === []) {
                    throw new Exception("'$key' in 'having' must be a non-empty object or list of conditions.");
                }

                $operands = array_is_list($value) ? $value : [$value];
                foreach ($operands as $operand) {
                    if (!is_array($operand)) {
                        throw new Exception("'$key' in 'having' must contain object conditions.");
                    }

                    self::collectGroupByHavingValidationIssues($operand, $by, $fields, $modelName, $missingFields);
                }

                continue;
            }

            if (!isset($fields[$key])) {
                throw new Exception("Field '$key' does not exist in {$modelName}.");
            }

            if (!is_array($value) || $value === []) {
                if (!in_array($key, $by, true)) {
                    $missingFields[] = $key;
                }

                continue;
            }

            $usesDirectFieldFilters = false;

            foreach ($value as $filterKey => $filterValue) {
                if (in_array($filterKey, $aggregateKeys, true)) {
                    if (!is_array($filterValue) || $filterValue === []) {
                        throw new Exception("Aggregate filter '$filterKey' in 'having' for field '$key' must be a non-empty object.");
                    }

                    foreach (array_keys($filterValue) as $operator) {
                        if (!self::isGroupByHavingScalarOperator($operator)) {
                            throw new Exception("Unsupported operator '$operator' in aggregate filter '$filterKey' for field '$key'.");
                        }
                    }

                    continue;
                }

                if (!self::isGroupByHavingScalarOperator($filterKey)) {
                    throw new Exception("Unsupported filter '$filterKey' in 'having' for field '$key'.");
                }

                $usesDirectFieldFilters = true;
            }

            if ($usesDirectFieldFilters && !in_array($key, $by, true)) {
                $missingFields[] = $key;
            }
        }
    }

    private static function buildHavingConditionGroups(
        array $having,
        array $aggMap,
        string $dbType,
        string $quotedTableName,
        array &$bindings,
        string $bindingPrefix = 'having'
    ): array {
        $clauses = [];

        foreach ($having as $key => $value) {
            if (self::isGroupByHavingLogicalOperator($key)) {
                $operands = array_is_list($value) ? $value : [$value];
                $operandGroups = [];

                foreach ($operands as $index => $operand) {
                    if (!is_array($operand)) {
                        continue;
                    }

                    $operandClauses = self::buildHavingConditionGroups(
                        $operand,
                        $aggMap,
                        $dbType,
                        $quotedTableName,
                        $bindings,
                        $bindingPrefix . '_' . strtolower($key) . '_' . $index
                    );

                    if ($operandClauses === []) {
                        continue;
                    }

                    $operandGroups[] = count($operandClauses) === 1
                        ? $operandClauses[0]
                        : '(' . implode(' AND ', $operandClauses) . ')';
                }

                if ($operandGroups === []) {
                    continue;
                }

                if ($key === 'NOT') {
                    $combined = count($operandGroups) === 1
                        ? $operandGroups[0]
                        : '(' . implode(' AND ', $operandGroups) . ')';
                    $clauses[] = 'NOT (' . $combined . ')';
                } else {
                    $clauses[] = '(' . implode(" {$key} ", $operandGroups) . ')';
                }

                continue;
            }

            $fieldClauses = self::buildFieldHavingConditions(
                $key,
                $value,
                $aggMap,
                $dbType,
                $quotedTableName,
                $bindings,
                $bindingPrefix . '_' . $key
            );

            if ($fieldClauses === []) {
                continue;
            }

            $clauses[] = count($fieldClauses) === 1
                ? $fieldClauses[0]
                : '(' . implode(' AND ', $fieldClauses) . ')';
        }

        return $clauses;
    }

    private static function buildFieldHavingConditions(
        string $field,
        mixed $value,
        array $aggMap,
        string $dbType,
        string $quotedTableName,
        array &$bindings,
        string $bindingPrefix
    ): array {
        $fieldQuoted = self::quoteColumnName($dbType, $field);
        $fieldExpression = $quotedTableName . '.' . $fieldQuoted;

        if (!is_array($value) || $value === []) {
            return self::buildHavingComparatorClauses(
                $fieldExpression,
                ['equals' => $value],
                $bindings,
                $bindingPrefix
            );
        }

        $scalarComparators = [];
        $aggregateComparators = [];

        foreach ($value as $filterKey => $filterValue) {
            if (isset($aggMap[$filterKey])) {
                $aggregateComparators[$filterKey] = $filterValue;
                continue;
            }

            $scalarComparators[$filterKey] = $filterValue;
        }

        $clauses = [];

        if ($scalarComparators !== []) {
            $clauses = array_merge(
                $clauses,
                self::buildHavingComparatorClauses($fieldExpression, $scalarComparators, $bindings, $bindingPrefix . '_field')
            );
        }

        foreach ($aggregateComparators as $aggregateKey => $comparators) {
            $aggregateExpression = $aggregateKey === '_count'
                ? 'COUNT(' . $fieldExpression . ')'
                : $aggMap[$aggregateKey] . '(' . $fieldExpression . ')';

            $clauses = array_merge(
                $clauses,
                self::buildHavingComparatorClauses(
                    $aggregateExpression,
                    $comparators,
                    $bindings,
                    $bindingPrefix . '_' . ltrim($aggregateKey, '_')
                )
            );
        }

        return $clauses;
    }

    private static function buildHavingComparatorClauses(
        string $expression,
        mixed $comparators,
        array &$bindings,
        string $bindingPrefix
    ): array {
        if (!is_array($comparators)) {
            $comparators = ['equals' => $comparators];
        }

        $clauses = [];

        foreach ($comparators as $operator => $value) {
            if ($operator === 'not') {
                if (is_array($value)) {
                    $nestedClauses = self::buildHavingComparatorClauses(
                        $expression,
                        $value,
                        $bindings,
                        $bindingPrefix . '_not'
                    );

                    if ($nestedClauses !== []) {
                        $clauses[] = 'NOT (' . implode(' AND ', $nestedClauses) . ')';
                    }

                    continue;
                }

                if ($value === null) {
                    $clauses[] = $expression . ' IS NOT NULL';
                    continue;
                }

                $placeholder = ':' . $bindingPrefix . '_' . count($bindings);
                $bindings[$placeholder] = $value;
                $clauses[] = $expression . ' <> ' . $placeholder;
                continue;
            }

            if ($operator === 'equals' && $value === null) {
                $clauses[] = $expression . ' IS NULL';
                continue;
            }

            $sqlOperator = self::sqlOperator($operator);

            if ($sqlOperator === 'BETWEEN') {
                if (!is_array($value) || count($value) !== 2) {
                    throw new Exception("Operator 'between' expects exactly two values in 'having'.");
                }

                $startPlaceholder = ':' . $bindingPrefix . '_' . count($bindings) . '_a';
                $endPlaceholder = ':' . $bindingPrefix . '_' . count($bindings) . '_b';
                $bindings[$startPlaceholder] = $value[0];
                $bindings[$endPlaceholder] = $value[1];
                $clauses[] = $expression . ' BETWEEN ' . $startPlaceholder . ' AND ' . $endPlaceholder;
                continue;
            }

            if (in_array($sqlOperator, ['IN', 'NOT IN'], true)) {
                if (!is_array($value) || $value === []) {
                    throw new Exception("Operator '$operator' expects a non-empty array in 'having'.");
                }

                $placeholders = [];
                foreach ($value as $item) {
                    $placeholder = ':' . $bindingPrefix . '_' . count($bindings);
                    $bindings[$placeholder] = $item;
                    $placeholders[] = $placeholder;
                }

                $clauses[] = $expression . ' ' . $sqlOperator . ' (' . implode(', ', $placeholders) . ')';
                continue;
            }

            $placeholder = ':' . $bindingPrefix . '_' . count($bindings);
            $bindings[$placeholder] = $value;
            $clauses[] = $expression . ' ' . $sqlOperator . ' ' . $placeholder;
        }

        return $clauses;
    }

    public static function buildHavingClause(
        array  $having,
        array  $aggMap,
        string $dbType,
        array  &$bindings,
        string $quotedTableName
    ): string {
        if ($having === []) {
            return '';
        }

        $clauses = self::buildHavingConditionGroups($having, $aggMap, $dbType, $quotedTableName, $bindings);

        return $clauses ? ' HAVING ' . implode(' AND ', $clauses) : '';
    }

    public static function bindValues(PDOStatement $stmt, array $bindings): void
    {
        foreach ($bindings as $key => $value) {
            if (is_int($value)) {
                $stmt->bindValue($key, $value, PDO::PARAM_INT);
                continue;
            }

            if (is_bool($value)) {
                $stmt->bindValue($key, $value, PDO::PARAM_BOOL);
                continue;
            }

            if ($value === null) {
                $stmt->bindValue($key, null, PDO::PARAM_NULL);
                continue;
            }

            $stmt->bindValue($key, $value);
        }
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
