package com.caffinc.jaggr.core;

import com.caffinc.jaggr.core.operations.CountOperation;
import com.caffinc.jaggr.utils.JsonFileUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests the <code>BatchAggregation</code>
 *
 * @author Sriram
 * @since 11/29/2016
 */
public class BatchAggregationTest {
    private static final List<Map<String, Object>> jsonData = new ArrayList<>();

    @BeforeClass
    public static void setUp() throws Exception {
        jsonData.addAll(JsonFileUtil.readJsonFromResource("raw.json"));
    }

    @Test
    public void testAggregateBatch() throws Exception {
        BatchAggregation aggregation = new AggregationBuilder()
                .setGroupBy("f")
                .addOperation("count", new CountOperation())
                .getBatchAggregation();
        List<Map<String, Object>> intermediateResult = aggregation
                .aggregateBatch(jsonData)
                .getItermediateResult();
        List<Map<String, Object>> finalResult = aggregation
                .aggregateBatch(jsonData)
                .getFinalResult();
        Assert.assertEquals("Result of grouping for intermediate batch and final batch should for duplicate data " +
                "should have the same size", intermediateResult.size(), finalResult.size());

        for (int i = 0; i < intermediateResult.size(); i++) {
            Assert.assertEquals("Counts should be doubled when aggregating same data twice",
                    (int) intermediateResult.get(0).get("count") * 2, finalResult.get(0).get("count"));
        }

        List<Map<String, Object>> postItermediateResult = aggregation.getItermediateResult();
        Assert.assertEquals("Intermediate result after final result should be empty", 0, postItermediateResult.size());
        List<Map<String, Object>> postFinalResult = aggregation.getFinalResult();
        Assert.assertEquals("Final result after final result should be empty", 0, postFinalResult.size());
    }
}
