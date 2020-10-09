<?php
declare(strict_types=1);

namespace KikCmsCore\Exceptions;


use Exception;

class ResourcesExceededException extends Exception
{
    protected $message = 'Resource usage exceeded';
}