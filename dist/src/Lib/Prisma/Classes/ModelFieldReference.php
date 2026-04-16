<?php

declare(strict_types=1);

namespace Lib\Prisma\Classes;

final class ModelFieldReference
{
    public function __construct(
        private string $modelName,
        private string $fieldName,
        private ?array $fieldMeta = null,
    ) {
    }

    public function modelName(): string
    {
        return $this->modelName;
    }

    public function fieldName(): string
    {
        return $this->fieldName;
    }

    public function fieldMeta(): ?array
    {
        return $this->fieldMeta;
    }
}
