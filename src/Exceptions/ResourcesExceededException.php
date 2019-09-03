<?php
declare(strict_types=1);

namespace KikCmsCore\Exceptions;


class ResourcesExceededException extends \Exception
{
    protected $message = 'Resource usage exceeded';
}