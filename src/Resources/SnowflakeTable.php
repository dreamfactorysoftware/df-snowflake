<?php
namespace DreamFactory\Core\Snowflake\Resources;

use DB;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Enums\Verbs;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\BatchException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\SqlDb\Resources\Table as MySqlTable;
use DreamFactory\Core\Utility\Session;

class SnowflakeTable extends MySqlTable
{
    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        $dbConn = $this->parent->getConnection();
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            if (0 < $dbConn->transactionLevel()) {
                $dbConn->commit();
            }

            return null;
        }

        $updates = array_get($extras, 'updates');
        $ssFilters = array_get($extras, 'ss_filters');
        $related = array_get($extras, 'related');
        $requireMore = array_get_bool($extras, 'require_more') || !empty($related);
        $allowRelatedDelete = array_get_bool($extras, 'allow_related_delete');
        $relatedInfo = $this->describeTableRelated($this->transactionTable);

        $builder = $dbConn->table($this->transactionTableSchema->internalName);

        /** @type ColumnSchema $idName */
        $idName = (isset($this->tableIdsInfo, $this->tableIdsInfo[0])) ? $this->tableIdsInfo[0] : null;
        if (empty($idName)) {
            throw new BadRequestException('No valid identifier found for this table.');
        }

        if (!empty($this->batchRecords)) {
            if (is_array($this->batchRecords[0])) {
                $temp = [];
                foreach ($this->batchRecords as $record) {
                    $temp[] = array_get($record, $idName->getName(true));
                }

                $builder->whereIn($idName->name, $temp);
            } else {
                $builder->whereIn($idName->name, $this->batchRecords);
            }
        } else {
            $builder->whereIn($idName->name, $this->batchIds);
        }

        $serverFilter = $this->buildQueryStringFromData($ssFilters);
        if (!empty($serverFilter)) {
            Session::replaceLookups($serverFilter);
            $params = [];
            $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
            $builder->whereRaw($filterString, $params);
        }

        $out = [];
        $action = $this->getAction();
        if (!empty($this->batchRecords)) {
            if (1 == count($this->tableIdsInfo)) {
                // records are used to retrieve extras
                // ids array are now more like records
                $result = $this->runQuery($this->transactionTable, $builder, $extras);
                if (empty($result)) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                $out = $result;
            } else {
                $out = $this->retrieveRecords($this->transactionTable, $this->batchRecords, $extras);
            }

            $this->batchRecords = [];
        } elseif (!empty($this->batchIds)) {
            switch ($action) {
                case Verbs::PUT:
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                        if (!empty($parsed)) {
                            $rows = $builder->update($parsed);
                            if (count($this->batchIds) !== $rows) {
                                throw new BadRequestException('Batch Error: Not all requested records could be updated.');
                            }
                        }

                        foreach ($this->batchIds as $id) {
                            if (!empty($relatedInfo)) {
                                $this->updatePostRelations(
                                    $this->transactionTable,
                                    array_merge($updates, [$idName->getName(true) => $id]),
                                    $relatedInfo,
                                    $allowRelatedDelete
                                );
                            }
                        }

                        if ($requireMore) {
                            $result = $this->runQuery(
                                $this->transactionTable,
                                $builder,
                                $extras
                            );

                            $out = $result;
                        }
                    }
                    break;

                case Verbs::DELETE:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );
                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == array_get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }
                    } else {
                        $out = $result;
                    }

                    $rows = $builder->delete();
                    if (count($this->batchIds) !== $rows) {
                        throw new BatchException($out, 'Batch Error: Not all requested records could be deleted.');
                    }
                    break;

                case Verbs::GET:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );

                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == array_get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }

                        throw new BatchException($out, 'Batch Error: Not all requested records could be retrieved.');
                    }

                    $out = $result;
                    break;

                default:
                    break;
            }

            if (empty($out)) {
                $out = [];
                foreach ($this->batchIds as $id) {
                    $out[] = [$idName->getName(true) => $id];
                }
            }

            $this->batchIds = [];
        }

        if (0 < $dbConn->transactionLevel()) {
            $dbConn->commit();
        }

        return $out;
    }

}