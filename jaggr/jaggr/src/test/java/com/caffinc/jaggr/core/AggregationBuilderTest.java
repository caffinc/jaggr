package com.caffinc.jaggr.core;

import com.caffinc.jaggr.core.operations.CountOperation;
import com.caffinc.jaggr.core.operations.Operation;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests the <code>AggregationBuilder</code>
 *
 * @author Sriram
 * @since 11/29/2016
 */
public class AggregationBuilderTest {
    private static final Gson gson = new Gson();

    @Test
    public void testGetAggregation() throws Exception {
        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy("dummy")
                .addOperation("op", new CountOperation())
                .getAggregation();
        Field groupField = Aggregation.class.getDeclaredField("_id");
        groupField.setAccessible(true);
        Assert.assertEquals("Aggregation ID must match GroupBy set", "dummy", groupField.get(aggregation));
        Field operationField = Aggregation.class.getDeclaredField("operationMap");
        operationField.setAccessible(true);
        Map<String, Operation> operationMap = new HashMap<>();
        operationMap.put("op", new CountOperation());
        Assert.assertEquals("Operations must match operations added",
                gson.toJson(operationMap), gson.toJson(operationField.get(aggregation)));
    }

    @Test
    public void testGetBatchAggregation() throws Exception {
        BatchAggregation aggregation = new AggregationBuilder()
                .setGroupBy("dummy")
                .addOperation("op", new CountOperation())
                .getBatchAggregation();
        Field groupField = BatchAggregation.class.getDeclaredField("_id");
        groupField.setAccessible(true);
        Assert.assertEquals("Aggregation ID must match GroupBy set", "dummy", groupField.get(aggregation));
        Field operationField = BatchAggregation.class.getDeclaredField("operationMap");
        operationField.setAccessible(true);
        Map<String, Operation> operationMap = new HashMap<>();
        operationMap.put("op", new CountOperation());
        Assert.assertEquals("Operations must match operations added",
                gson.toJson(operationMap), gson.toJson(operationField.get(aggregation)));
    }
}
