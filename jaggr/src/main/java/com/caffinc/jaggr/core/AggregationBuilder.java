package com.caffinc.jaggr.core;


import com.caffinc.jaggr.core.operations.Operation;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for Aggregations
 *
 * @author Sriram
 * @since 11/26/2016
 */
public class AggregationBuilder {
    private String _id = null;
    private Map<String, Operation> operationMap = new HashMap<>();

    public AggregationBuilder setGroupBy(String field) {
        _id = field;
        return this;
    }

    public AggregationBuilder addOperation(String field, Operation operation) {
        operationMap.put(field, operation);
        return this;
    }

    public Aggregation getAggregation() {
        return new Aggregation(_id, operationMap);
    }
}
