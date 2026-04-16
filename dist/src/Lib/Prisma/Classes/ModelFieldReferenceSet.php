<?php

declare(strict_types=1);

namespace Lib\Prisma\Classes;

use Exception;

final class ModelFieldReferenceSet
{
    public function __construct(
        private string $modelName,
        private array $fields,
    ) {
    }

    public function __get(string $name): ModelFieldReference
    {
        $fieldMeta = $this->fields[$name] ?? null;

        if (!is_array($fieldMeta)) {
            throw new Exception("Field '$name' does not exist in {$this->modelName}.fields.");
        }

        if (!in_array($fieldMeta['kind'] ?? null, ['scalar', 'enum'], true)) {
            throw new Exception("Field '$name' in {$this->modelName}.fields is not a scalar or enum field reference.");
        }

        return new ModelFieldReference($this->modelName, $name, $fieldMeta);
    }

    public function __isset(string $name): bool
    {
        return is_array($this->fields[$name] ?? null);
    }
}
