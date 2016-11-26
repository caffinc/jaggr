package com.caffinc.jaggr.core;

import com.caffinc.jaggr.core.operations.Operation;
import com.caffinc.jaggr.core.utils.FieldValueExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates list of objects based on operations
 *
 * @author Sriram
 * @since 11/26/2016
 */
public class Aggregation {
    private String _id;
    private String[] idSplit;
    private Map<String, Operation> operationMap;

    Aggregation(String _id, Map<String, Operation> operationMap) {
        this._id = _id;
        this.idSplit = _id != null ? _id.split("\\.") : null;
        this.operationMap = operationMap;
    }

    public List<Map<String, Object>> aggregate(List<Map<String, Object>> objects) {
        Map<String, Map<String, Object>> workspace = new HashMap<>();
        for (Map<String, Object> object : objects) {
            String id = "0";
            if (_id != null) {
                id = String.valueOf(FieldValueExtractor.getValue(idSplit, object));
            }
            if (!workspace.containsKey(id)) {
                Map<String, Object> groupWorkspace = new HashMap<>();
                groupWorkspace.put("_id", id);
                workspace.put(id, groupWorkspace);
            }
            Map<String, Object> groupWorkspace = workspace.get(id);
            for (Map.Entry<String, Operation> operationEntry : operationMap.entrySet()) {
                String field = operationEntry.getKey();
                Operation operation = operationEntry.getValue();
                Object t0 = groupWorkspace.get(field);
                Object t1 = operation.aggregate(t0, object);
                groupWorkspace.put(field, t1);
            }
        }
        List<Map<String, Object>> resultList = new ArrayList<>();
        for (Map<String, Object> groupWorkspace : workspace.values()) {
            for (Map.Entry<String, Operation> operationEntry : operationMap.entrySet()) {
                String field = operationEntry.getKey();
                Operation operation = operationEntry.getValue();
                groupWorkspace.put(field, operation.result(groupWorkspace.get(field)));
            }
            resultList.add(groupWorkspace);
        }
        return resultList;
    }
}
