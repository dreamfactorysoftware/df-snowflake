<?php
namespace DreamFactory\Core\Compliance\Enums;

use DreamFactory\Core\Enums\VerbsMask as CoreVerbsMask;

/**
 * Various REST verbs as bitmask-able values
 *
 * This one is overriding the VerbMask from df-core
 */
class VerbsMask extends CoreVerbsMask
{
    /**
     * @return int
     */
    public static function getFullAccessMask()
    {
        return
            self::GET_MASK |
            self::POST_MASK |
            self::PUT_MASK |
            self::PATCH_MASK |
            self::DELETE_MASK;
    }
}
