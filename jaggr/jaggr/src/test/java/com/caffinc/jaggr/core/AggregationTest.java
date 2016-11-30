package com.caffinc.jaggr.core;

import com.caffinc.jaggr.core.operations.*;
import com.caffinc.jaggr.core.utils.FieldValueExtractor;
import com.caffinc.jaggr.utils.JsonFileUtil;
import com.google.gson.Gson;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Tests the Aggregation framework using the <code>Aggregation</code> class
 *
 * @author Sriram
 * @since 11/26/2016
 */
public class AggregationTest {
    private static final Gson gson = new Gson();
    private static List<Map<String, Object>> jsonList;

    private static <T> T roughen(Object o, Class<T> t) {
        return gson.fromJson(gson.toJson(o), t);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        jsonList = JsonFileUtil.readJsonFromResource("raw.json");
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
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("countResult.json"));

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
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("maxResult.json"));

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
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("minResult.json"));

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
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("collectResult.json"));

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
        for (Map<String, Object> expectedObject : JsonFileUtil.readJsonFromResource("collectSetResult.json")) {
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
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("sumResult.json"));

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
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("avgResult.json"));

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("avg", new AverageOperation(avgField))
                .getAggregation();
        Set<Map<String, Object>> result = roughen(aggregation.aggregate(jsonList), HashSet.class);
        assertEquals("Average should be as expected", expected, result);
    }

    @Test
    public void testOperationWithoutGrouping() throws Exception {
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("grouplessResult.json"));

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
        Set<Map<String, Object>> expected = new HashSet<>(JsonFileUtil.readJsonFromResource("multiResult.json"));

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
        Map<String, Object> expectedMap1 = new HashMap<>();
        Map<String, Object> expectedMap2 = new HashMap<>();
        for (Map<String, Object> expectedObject : JsonFileUtil.readJsonFromResource("collectStringsResult.json")) {
            expectedMap1.put(String.valueOf(expectedObject.get("_id")), new HashSet((List) expectedObject.get("set")));
            expectedMap2.put(String.valueOf(expectedObject.get("_id")), expectedObject.get("list"));
        }

        Aggregation aggregation = new AggregationBuilder()
                .setGroupBy(field)
                .addOperation("list", new CollectOperation(collectField))
                .addOperation("set", new CollectSetOperation(collectField))
                .getAggregation();
        Map<String, Object> resultMap1 = new HashMap<>();
        Map<String, Object> resultMap2 = new HashMap<>();
        for (Map<String, Object> resultObject : (Set<Map<String, Object>>) roughen(aggregation.aggregate(jsonList), HashSet.class)) {
            resultMap1.put(String.valueOf(resultObject.get("_id")), new HashSet((List) resultObject.get("set")));
            resultMap2.put(String.valueOf(resultObject.get("_id")), resultObject.get("list"));
        }

        assertEquals("Collect for Strings should work as expected", expectedMap1, resultMap1);
        assertEquals("CollectSet for Strings should work as expected", expectedMap2, resultMap2);
    }


    @Test
    public void testIterativeAggregation() throws Exception {
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
    public void testFirstObjectOperation() throws Exception {
        List<Map<String, Object>> expected = JsonFileUtil.readJsonFromResource("raw.json");

        Aggregation aggregation = new AggregationBuilder()
                .addOperation("first", new FirstObjectOperation("test.f"))
                .getAggregation();
        List<Map<String, Object>> result = aggregation.aggregate(jsonList);
        assertEquals("First object should be as expected",
                ((Map) expected.get(0).get("test")).get("f"),
                result.get(0).get("first"));
    }

    @Test
    public void testLastObjectOperation() throws Exception {
        List<Map<String, Object>> expected = JsonFileUtil.readJsonFromResource("raw.json");

        Aggregation aggregation = new AggregationBuilder()
                .addOperation("last", new LastObjectOperation("test.f"))
                .getAggregation();
        List<Map<String, Object>> result = aggregation.aggregate(jsonList);
        assertEquals("Last object should be as expected",
                ((Map) expected.get(expected.size() - 1).get("test")).get("f"),
                result.get(0).get("last"));
    }

    @Test
    public void testStdDevOperation() throws Exception {
        List<Map<String, Object>> expected = JsonFileUtil.readJsonFromResource("raw.json");
        double[] doubleList = new double[expected.size()];
        int i = 0;
        for (Map<String, Object> o : expected) {
            doubleList[i++] = (double) ((Map) o.get("test")).get("f");
        }
        double stddev = new StandardDeviation().evaluate(doubleList);
        Aggregation aggregation = new AggregationBuilder()
                .addOperation("stddev", new StdDevPopOperation("test.f"))
                .getAggregation();
        List<Map<String, Object>> result = aggregation.aggregate(jsonList);
        assertEquals("Standard Deviation should be as expected",
                stddev,
                result.get(0).get("stddev"));
    }


    @Test
    public void testTopNOperation() throws Exception {
        Comparator<Double> comparator = new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                return Double.compare(o1, o2);
            }
        };
        List<Map<String, Object>> expected = JsonFileUtil.readJsonFromResource("raw.json");
        List<Double> doubleList = new ArrayList<>();
        for (Map<String, Object> o : expected) {
            doubleList.add((double) ((Map) o.get("test")).get("f"));
        }
        Collections.sort(doubleList, comparator);
        Aggregation aggregation = new AggregationBuilder()
                .addOperation("topn", new TopNOperation<>("test.f", 3, comparator))
                .getAggregation();
        List<Double> result = (List<Double>) aggregation.aggregate(jsonList).get(0).get("topn");
        Collections.sort(result, comparator);
        for (int i = 0; i < result.size(); i++) {
            assertEquals("Top N should be as expected",
                    doubleList.get(doubleList.size() - 1 - i),
                    result.get(result.size() - 1 - i));
        }
    }
}
