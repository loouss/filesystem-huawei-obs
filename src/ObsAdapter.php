<?php

declare(strict_types=1);

namespace Loouss\Filesystem\Obs;

class ObsAdapter
{
    public function make(array $options): Adapter
    {
        return new Adapter($options);
    }
}