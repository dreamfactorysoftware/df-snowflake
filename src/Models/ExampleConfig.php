<?php

namespace DreamFactory\Core\Skeleton\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;
use Illuminate\Support\Arr;

/**
 * Write your model
 *
 * Write your methods, properties or override ones from the parent
 *
 */
class ExampleConfig extends BaseServiceConfigModel
{
    /** @var string */
    protected $table = 'db_example';

    /** @var array */
    protected $fillable = [
        'service_id',
        'label',
        'description',
        'is_example'
    ];

    /** @var array */
    protected $casts = [
        'service_id' => 'integer',
        'is_example' => 'boolean'
    ];

    /**
     * The attributes excluded from the model's JSON form.
     *
     * @var array
     */
    protected $hidden = ['created_date'];

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'label':
                $schema['label'] = 'Simple label';
                $schema['type'] = 'text';
                $schema['required'] = false;
                $schema['description'] = 'This is just a simple label';
                break;

            case 'description':
                $schema['label'] = 'Description';
                $schema['type'] = 'text';
                $schema['description'] =
                    'This is just a description';
                break;
            case 'is_example':
                $schema['label'] = 'Is this an example?';
                $schema['type'] = 'boolean';
                $schema['description'] =
                    'It must be an example';
                break;
        }
    }


}