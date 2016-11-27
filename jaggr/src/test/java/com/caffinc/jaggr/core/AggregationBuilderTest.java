package com.caffinc.jaggr.core;

import com.caffinc.jaggr.core.operations.*;
import com.caffinc.jaggr.core.utils.FieldValueExtractor;
import com.caffinc.jaggr.core.utils.JsonFileReader;
import com.google.gson.Gson;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Tests the Aggregation Builder
 *
 * @author Sriram
 * @since 11/26/2016
 */
public class AggregationBuilderTest {
    private static final Gson gson = new Gson();
    private static List<Map<String, Object>> jsonList;

    private static <T> T roughen(Object o, Class<T> t) {
        return gson.fromJson(gson.toJson(o), t);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        jsonList = JsonFileReader.readJsonFromResource("raw.json");
    }

    @Test
    public void testSimpleGrouping() throws Exception {
        String field = "f";
        Set<Object> expectedResult = new HashSet<>();
        for (Map<String, Object> obj : jsonList) {
            expectedResult.add(String.valueOf(obj.get(field)));
        }

        Set<Object> result = new HashSet<>();
        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field).getAggregation();
        List<Map<String, Object>> resultList = aggregation.aggregate(jsonList);
        for (Map<String, Object> resultObj : resultList) {
            result.add(resultObj.get("_id"));
        }

        assertEquals("Grouping by ID should match", expectedResult, result);
    }

    @Test
    public void testNestedGrouping() throws Exception {
        String field = "test.f";
        Set<Object> expectedResult = new HashSet<>();
        for (Map<String, Object> obj : jsonList) {
            expectedResult.add(String.valueOf(FieldValueExtractor.getValue(field, obj)));
        }

        Set<Object> result = new HashSet<>();
        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field).getAggregation();
        List<Map<String, Object>> resultList = aggregation.aggregate(jsonList);
        for (Map<String, Object> resultObj : resultList) {
            result.add(resultObj.get("_id"));
        }

        assertEquals("Grouping by field should match", expectedResult, result);
    }

    @Test
    public void testCountOperation() throws Exception {
        String field = "f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("countResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("count", new CountOperation())
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Counts should be as expected", expected, result);
    }

    @Test
    public void testMaxOperation() throws Exception {
        String field = "f";
        String maxField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("maxResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("max", new MaxOperation(maxField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Max should be as expected", expected, result);
    }

    @Test
    public void testMinOperation() throws Exception {
        String field = "f";
        String minField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("minResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("min", new MinOperation(minField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Min should be as expected", expected, result);
    }

    @Test
    public void testCollectOperation() throws Exception {
        String field = "f";
        String collectField = "_id";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("collectResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("list", new CollectOperation(collectField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Collected lists should be as expected", expected, result);
    }

    @Test
    public void testCollectSetOperation() throws Exception {
        String field = "f";
        String collectField = "test.f";
        Map<String, Object> expectedMap = new HashMap<>();
        for (Map<String, Object> expectedObject : JsonFileReader.readJsonFromResource("collectSetResult.json")) {
            expectedMap.put(String.valueOf(expectedObject.get("_id")), new HashSet((List) expectedObject.get("set")));
        }

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("set", new CollectSetOperation(collectField))
                .getAggregation();
        Map<String, Object> resultMap = new HashMap<>();
        for (Map<String, Object> resultObject : (Set<Map<String, Object>>) roughen(aggregation.aggregate(jsonList), HashSet.class)) {
            resultMap.put(String.valueOf(resultObject.get("_id")), new HashSet((List) resultObject.get("set")));
        }
        assertEquals("Collected sets should be as expected", expectedMap, resultMap);
    }

    @Test
    public void testSumOperation() throws Exception {
        String field = "f";
        String sumField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("sumResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("sum", new SumOperation(sumField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Sum should be as expected", expected, result);
    }

    @Test
    public void testAverageOperation() throws Exception {
        String field = "f";
        String avgField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("avgResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("avg", new AverageOperation(avgField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Average should be as expected", expected, result);
    }

    @Test
    public void testOperationWithoutGrouping() throws Exception {
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("grouplessResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .addOperation("count", new CountOperation())
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Counts without grouping should be as expected", expected, result);
    }

    @Test
    public void testMultiOperation() throws Exception {
        String field = "f";
        String avgField = "test.f";
        String sumField = "test.f";
        String minField = "test.f";
        String maxField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("multiResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("avg", new AverageOperation(avgField))
                .addOperation("sum", new SumOperation(sumField))
                .addOperation("min", new MinOperation(minField))
                .addOperation("max", new MaxOperation(maxField))
                .addOperation("count", new CountOperation())
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Multiple aggregations result should be as expected", expected, result);
    }

    @Test
    public void testCollectStrings() throws Exception {
        String field = "test.f";
        String collectField = "f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("collectStringsResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("list", new CollectOperation(collectField))
                .addOperation("set", new CollectSetOperation(collectField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Collect for Strings should work as expected", expected, result);
    }


    @Test
    public void testIterativeSimpleGrouping() throws Exception {
        String field = "f";
        Set<Object> expectedResult = new HashSet<>();
        for (Map<String, Object> obj : jsonList) {
            expectedResult.add(String.valueOf(obj.get(field)));
        }

        Set<Object> result = new HashSet<>();
        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field).getAggregation();
        List<Map<String, Object>> resultList = aggregation.aggregate(jsonList.iterator());
        for (Map<String, Object> resultObj : resultList) {
            result.add(resultObj.get("_id"));
        }

        assertEquals("Grouping by ID should match", expectedResult, result);
    }

    @Test
    public void testIterativeNestedGrouping() throws Exception {
        String field = "test.f";
        Set<Object> expectedResult = new HashSet<>();
        for (Map<String, Object> obj : jsonList) {
            expectedResult.add(String.valueOf(FieldValueExtractor.getValue(field, obj)));
        }

        Set<Object> result = new HashSet<>();
        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field).getAggregation();
        List<Map<String, Object>> resultList = aggregation.aggregate(jsonList.iterator());
        for (Map<String, Object> resultObj : resultList) {
            result.add(resultObj.get("_id"));
        }

        assertEquals("Grouping by field should match", expectedResult, result);
    }

    @Test
    public void testIterativeCountOperation() throws Exception {
        String field = "f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("countResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("count", new CountOperation())
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Counts should be as expected", expected, result);
    }

    @Test
    public void testIterativeMaxOperation() throws Exception {
        String field = "f";
        String maxField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("maxResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("max", new MaxOperation(maxField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Max should be as expected", expected, result);
    }

    @Test
    public void testIterativeMinOperation() throws Exception {
        String field = "f";
        String minField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("minResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("min", new MinOperation(minField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Min should be as expected", expected, result);
    }

    @Test
    public void testIterativeCollectOperation() throws Exception {
        String field = "f";
        String collectField = "_id";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("collectResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("list", new CollectOperation(collectField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Collected lists should be as expected", expected, result);
    }

    @Test
    public void testIterativeCollectSetOperation() throws Exception {
        String field = "f";
        String collectField = "test.f";
        Map<String, Object> expectedMap = new HashMap<>();
        for (Map<String, Object> expectedObject : JsonFileReader.readJsonFromResource("collectSetResult.json")) {
            expectedMap.put(String.valueOf(expectedObject.get("_id")), new HashSet((List) expectedObject.get("set")));
        }

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("set", new CollectSetOperation(collectField))
                .getAggregation();
        Map<String, Object> resultMap = new HashMap<>();
        for (Map<String, Object> resultObject : (Set<Map<String, Object>>) roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class)) {
            resultMap.put(String.valueOf(resultObject.get("_id")), new HashSet((List) resultObject.get("set")));
        }
        assertEquals("Collected sets should be as expected", expectedMap, resultMap);
    }

    @Test
    public void testIterativeSumOperation() throws Exception {
        String field = "f";
        String sumField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("sumResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("sum", new SumOperation(sumField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Sum should be as expected", expected, result);
    }

    @Test
    public void testIterativeAverageOperation() throws Exception {
        String field = "f";
        String avgField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("avgResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("avg", new AverageOperation(avgField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Average should be as expected", expected, result);
    }

    @Test
    public void testIterativeOperationWithoutGrouping() throws Exception {
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("grouplessResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .addOperation("count", new CountOperation())
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Counts without grouping should be as expected", expected, result);
    }

    @Test
    public void testIterativeMultiOperation() throws Exception {
        String field = "f";
        String avgField = "test.f";
        String sumField = "test.f";
        String minField = "test.f";
        String maxField = "test.f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("multiResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("avg", new AverageOperation(avgField))
                .addOperation("sum", new SumOperation(sumField))
                .addOperation("min", new MinOperation(minField))
                .addOperation("max", new MaxOperation(maxField))
                .addOperation("count", new CountOperation())
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Multiple aggregations result should be as expected", expected, result);
    }

    @Test
    public void testIterativeCollectStrings() throws Exception {
        String field = "test.f";
        String collectField = "f";
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileReader.readJsonFromResource("collectStringsResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("list", new CollectOperation(collectField))
                .addOperation("set", new CollectSetOperation(collectField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList.iterator()), HashSet.class);
        assertEquals("Collect for Strings should work as expected", expected, result);
    }
}
