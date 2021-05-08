package com.baidu.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.store.BackendFeatures;

public class ClickhouseFeatures implements BackendFeatures {

    @Override
    public boolean supportsScanToken() {
        return true;
    }

    @Override
    public boolean supportsScanKeyPrefix() {
        return false;
    }

    @Override
    public boolean supportsScanKeyRange() {
        return true;
    }

    @Override
    public boolean supportsQuerySchemaByName() {
        // MySQL support secondary index
        return true;
    }

    @Override
    public boolean supportsQueryByLabel() {
        // MySQL support secondary index
        return true;
    }

    @Override
    public boolean supportsQueryWithRangeCondition() {
        return true;
    }

    @Override
    public boolean supportsQueryWithOrderBy() {
        return true;
    }

    @Override
    public boolean supportsQueryWithContains() {
        return false;
    }

    @Override
    public boolean supportsQueryWithContainsKey() {
        return false;
    }

    @Override
    public boolean supportsQueryByPage() {
        return true;
    }

    @Override
    public boolean supportsQuerySortByInputIds() {
        return false;
    }

    @Override
    public boolean supportsDeleteEdgeByLabel() {
        return true;
    }

    @Override
    public boolean supportsUpdateVertexProperty() {
        return false;
    }

    @Override
    public boolean supportsMergeVertexProperty() {
        return false;
    }

    @Override
    public boolean supportsUpdateEdgeProperty() {
        return false;
    }

    @Override
    public boolean supportsTransaction() {
        // Clickhouse do not support tx
        return false;
    }

    @Override
    public boolean supportsNumberType() {
        return true;
    }

    @Override
    public boolean supportsAggregateProperty() {
        return false;
    }

    @Override
    public boolean supportsTtl() {
        return false;
    }

//    @Override
//    public boolean supportsOlapProperties() {
//        return false;
//    }
}
