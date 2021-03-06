<?php

namespace KikCmsCore\Config;


class DbConfig
{
    const ERROR_CODE_FK_CONSTRAINT_FAIL        = 1451;
    const ERROR_CODE_TOO_MANY_USER_CONNECTIONS = 1226;
    const ERROR_CODE_DUPLICATE_ENTRY           = 1062;
    const ERROR_CODE_UNKNOWN_DB                = 1049;
    const ERROR_CODE_ACCESS_DENIED             = 1045;

    const NAME_FIELD = 'name';

    const SQL_DATE_FORMAT     = 'Y-m-d';
    const SQL_DATETIME_FORMAT = 'Y-m-d H:i:s';
    const SQL_TIME_FORMAT     = 'H:i:s';

    const SQL_SORT_ASCENDING  = 'asc';
    const SQL_SORT_DESCENDING = 'desc';

    const SQL_SORT_DIRECTIONS = [
        self::SQL_SORT_ASCENDING,
        self::SQL_SORT_DESCENDING,
    ];
}