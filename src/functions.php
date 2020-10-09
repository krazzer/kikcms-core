<?php

/**
 * Add a value to an array after a certain key
 *
 * @param array $subject
 * @param mixed $keyToAddAfter
 * @param string $keyToAdd
 * @param $valueToAdd
 * @return array
 */
function array_add_after_key(array $subject, $keyToAddAfter, string $keyToAdd, $valueToAdd): array
{
    $index = array_search($keyToAddAfter, array_keys($subject));

    if ($index === false) {
        $index = count($subject);
    } else {
        $index++;
    }

    $newArrayPart1 = array_slice($subject, 0, $index, true);
    $newArrayPart2 = array_slice($subject, $index, null, true);

    $newArrayPart1[$keyToAdd] = $valueToAdd;

    return (array) ($newArrayPart1 + $newArrayPart2);
}

/**
 * Add a value to an array before a certain key
 *
 * @param array $subject
 * @param string $keyToAddAfter
 * @param string $keyToAdd
 * @param $valueToAdd
 * @return array
 */
function array_add_before_key(array $subject, string $keyToAddAfter, string $keyToAdd, $valueToAdd): array
{
    $index = array_search($keyToAddAfter, array_keys($subject));

    if ($index === false) {
        $index = count($subject);
    } else {
        $index++;
    }

    $newArrayPart1 = array_slice($subject, 0, $index - 1, true);
    $newArrayPart2 = array_slice($subject, $index - 1, null, true);

    $newArrayPart1[$keyToAdd] = $valueToAdd;

    return $newArrayPart1 + $newArrayPart2;
}

/**
 * @param $array
 * @param $oldKey
 * @param $newKey
 * @return array
 */
function array_change_key($array, $oldKey, $newKey): array
{
    if ( ! array_key_exists($oldKey, $array)) {
        return $array;
    }

    $keys = array_keys($array);

    $keys[array_search($oldKey, $keys)] = $newKey;

    return array_combine($keys, $array);
}

/**
 * Fetch a value from an array by key, and return null if it doesn't exist
 *
 * @param $key
 * @param array $array
 * @return mixed|null
 */
function array_key($key, array $array)
{
    if( ! array_key_exists($key, $array)){
        return null;
    }

    return $array[$key];
}

/**
 * Return the first key of an array
 *
 * @param array $array
 *
 * @return mixed
 */
function first_key(array $array)
{
    return array_keys($array)[0];
}

/**
 * Return the first value of an array
 *
 * @param array $array
 *
 * @return mixed
 */
function first(array $array)
{
    return array_values($array)[0];
}

/**
 * Return the last value of an array
 *
 * @param array $array
 *
 * @return mixed
 */
function last(array $array)
{
    return array_values(array_slice($array, -1))[0];
}

/**
 * Return the last key of an array
 *
 * @param array $array
 *
 * @return mixed
 */
function last_key(array $array)
{
    $keys = array_keys($array);

    return array_pop($keys);
}

/**
 * Log the given parameters
 */
function dlog()
{
    $args = func_get_args();

    if (count($args) === 1) {
        $args = $args[0];
    }

    error_log(print_r($args, true));
}

/**
 * @param float $microseconds
 * @param null $name
 */
function dlogtime(float $microseconds, $name = null)
{
    dlog(($name ? $name . ': ' : '') . ((microtime(true) - $microseconds) * 1000));
}

/**
 * @param string $haystack
 * @param string $needle
 *
 * @return string
 */
function str_startswith(string $haystack, string $needle): string
{
    $length = strlen($needle);
    return (substr($haystack, 0, $length) === $needle);
}

/**
 * @param string $haystack
 * @param string $needle
 *
 * @return string
 */
function str_endswith(string $haystack, string $needle): string
{
    $length = strlen($needle);

    if ($length == 0) {
        return true;
    }

    return (substr($haystack, -$length) === $needle);
}