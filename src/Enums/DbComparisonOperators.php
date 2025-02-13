<?php

namespace DreamFactory\Core\Snowflake\Enums;

use DreamFactory\Core\Enums\DbComparisonOperators as BaseDbComparisonOperators;


/**
 * DbComparisonOperators
 * DB server-side filter comparison operator string constants
 */
class DbComparisonOperators extends BaseDbComparisonOperators
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * @var string
     */
    const ILIKE = 'ILIKE';

    public static function getParsingOrder()
    {
        $baseParsingOrder = parent::getParsingOrder();
        $likePos = array_search(static::LIKE, $baseParsingOrder);
        array_splice($baseParsingOrder, $likePos, 0, static::ILIKE);
        return $baseParsingOrder;
    }
}
