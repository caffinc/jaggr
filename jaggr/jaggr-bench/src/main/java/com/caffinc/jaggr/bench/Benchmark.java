package com.caffinc.jaggr.bench;

import com.caffinc.jaggr.core.Aggregation;
import com.caffinc.jaggr.core.AggregationBuilder;
import com.caffinc.jaggr.core.operations.AverageOperation;
import com.caffinc.jaggr.core.operations.CountOperation;
import com.caffinc.jaggr.core.operations.StdDevPopOperation;
import com.caffinc.jaggr.core.operations.SumOperation;
import com.caffinc.jaggr.utils.JsonFileIterator;
import com.caffinc.jaggr.utils.JsonIterator;
import com.google.gson.Gson;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author srira
 * @since 11/28/2016
 */
public class Benchmark {
    private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);
    private static final int BATCH_SIZE = 10000;
    private static final DBCollection BENCHMARK_COLLECTION =
            new MongoClient("localhost").getDB("jaggr").getCollection("benchmark");
    private static final Random RANDOM = new Random();
    private static final String PLACEHOLDER =
            "STRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRING" +
                    "STRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRING" +
                    "STRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRINGSTRING";

    public static void main(String[] args) throws Exception {
//        generateDocuments(BENCHMARK_COLLECTION, 10000000, getFieldDefinitions());
        final Aggregation aggregation = new AggregationBuilder()
//                .setGroupBy("name")
//                .addOperation("groupedIds", new CollectSetOperation("_id"))
                .addOperation("sum", new SumOperation("appeal"))
                .addOperation("avg", new AverageOperation("age"))
                .addOperation("count", new CountOperation())
                .addOperation("stddev", new StdDevPopOperation("age"))
                .getAggregation();
        long startTime;
        List<Map<String, Object>> result;

//        LOG.info("Computing read time");
//        startTime = System.currentTimeMillis();
//        Iterator<Map<String, Object>> dbObjectIterator = new JsonIterator<DBObject>(BENCHMARK_COLLECTION.find().iterator()) {
//            @Override
//            public Map<String, Object> toJson(DBObject element) {
//                return element.toMap();
//            }
//        };
//        List<Map<String, Object>> inMemList = new ArrayList<>();
//        while (dbObjectIterator.hasNext()) {
//            inMemList.add(dbObjectIterator.next());
//        }
//        LOG.info("Read time: {}ms {} docs", (System.currentTimeMillis() - startTime), inMemList.size());

        LOG.info("Starting Mongo Cursor aggregation");
        startTime = System.currentTimeMillis();
        result = aggregation.aggregate(new JsonIterator<DBObject>(BENCHMARK_COLLECTION.find()) {
            @Override
            public Map<String, Object> toJson(DBObject element) {
                return element.toMap();
            }
        });
        LOG.info("Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());

        LOG.info("Result: " + new Gson().toJson(result));


//        LOG.info("Starting in-memory aggregation");
//        startTime = System.currentTimeMillis();
//        result = aggregation.aggregate(inMemList);
//        LOG.info("In-memory Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());
//
//        LOG.info("Starting multithreaded aggregation");
//        int docCount = (int) BENCHMARK_COLLECTION.count();
//        int threadCount = 10;
//        final int limit = Double.valueOf(Math.ceil(((double) docCount) / threadCount)).intValue();
//        final CountDownLatch latch = new CountDownLatch(docCount);
//        startTime = System.currentTimeMillis();
//        for (int i = 0; i < threadCount; i++) {
//            final int batchId = i;
//            new Thread() {
//                @Override
//                public void run() {
//                    runAggregation(latch, aggregation, limit, batchId);
//                }
//            }.start();
//        }
//        latch.await();
//        LOG.info("Multi-threaded Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());

        LOG.info("Starting native aggregation");
        startTime = System.currentTimeMillis();
        List<Map<String, Object>> mongoResults = new ArrayList<>();
        for (DBObject dbObject : new Iterable<DBObject>() {
            @Override
            public Iterator<DBObject> iterator() {
                return BENCHMARK_COLLECTION.aggregate(Arrays.asList(
                        new BasicDBObject("$group",
                                new BasicDBObject("_id", null)
//                                        .append("groupedIds", new BasicDBObject("$addToSet", "$_id"))
                                        .append("sum", new BasicDBObject("$sum", "$appeal"))
                                        .append("avg", new BasicDBObject("$avg", "$age"))
                                        .append("stddev", new BasicDBObject("$stdDevPop", "$age"))
                                        .append("count", new BasicDBObject("$sum", 1)))

                ), AggregationOptions.builder().allowDiskUse(true).build());
            }
        }) {
            mongoResults.add(dbObject.toMap());
        }
        LOG.info("Mongo Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), mongoResults.size());

        LOG.info("Result: " + new Gson().toJson(mongoResults));

        LOG.info("Aggregating file");
        startTime = System.currentTimeMillis();
        result = aggregation.aggregate(new JsonFileIterator("C:\\Users\\srira\\Documents\\caffinc\\playarea\\bm.json"));
        LOG.info("File Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());

        LOG.info("Reading file");
        startTime = System.currentTimeMillis();
        int count = 0;
        for (Map<String, Object> obj : new Iterable<Map<String, Object>>() {
            @Override
            public Iterator<Map<String, Object>> iterator() {
                try {
                    return new JsonFileIterator("C:\\Users\\srira\\Documents\\caffinc\\playarea\\bm.json");
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }) {
            count = Double.valueOf((double) obj.get("_id")).intValue();
        }
        LOG.info("File Read time: {}ms {} docs", (System.currentTimeMillis() - startTime), count);
    }

    private static void runAggregation(final CountDownLatch latch, final Aggregation aggregation, final int limit, final int batchId) {
        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> result = aggregation.aggregate(new JsonIterator<DBObject>(BENCHMARK_COLLECTION.find().skip(limit * batchId).limit(limit)) {
            @Override
            public Map<String, Object> toJson(DBObject element) {
                latch.countDown();
                return element.toMap();
            }
        });
        LOG.info("Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());
    }

    private static Map<String, String> getFieldDefinitions() {
        Map<String, String> fieldDefinitions = new HashMap<>();
        fieldDefinitions.put("_id", "$id");
        fieldDefinitions.put("name", "$string_10_5");
        fieldDefinitions.put("age", "$int_10_99");
        fieldDefinitions.put("sex", "$choice_m|f");
        fieldDefinitions.put("salary", "$int_100000_1000000");
        fieldDefinitions.put("appeal", "$double");
        return fieldDefinitions;
    }

    private static void generateDocuments(
            DBCollection benchmarkCollection, int count, Map<String, String> fieldDefinitions) {
        List<DBObject> dbObjectList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, Object> doc = new HashMap<>();
            for (Map.Entry<String, String> field : fieldDefinitions.entrySet()) {
                doc.put(field.getKey(), generateField(field.getValue(), field.getKey(), i));
            }
            dbObjectList.add(new BasicDBObject(doc));
            if (dbObjectList.size() >= BATCH_SIZE) {
                LOG.info("Generated " + (i + 1) + " documents");
                benchmarkCollection.insert(dbObjectList);
                dbObjectList = new ArrayList<>();
            }
        }
        if (dbObjectList.size() > 0) {
            LOG.info("Generated " + count + " documents");
            benchmarkCollection.insert(dbObjectList);
        }
    }

    private static Object generateField(String fieldDefinition, String field, Integer id) {
        if (fieldDefinition == null || !fieldDefinition.startsWith("$")) {
            return fieldDefinition;
        } else {
            String[] definitionParts = fieldDefinition.split("_");
            String type = definitionParts[0];
            int min;
            int max;
            switch (type) {
                case "$id":
                    return id;
                case "$string":
                    min = Integer.parseInt(definitionParts[1]);
                    max = Integer.parseInt(definitionParts[2]);
                    int minVal = Double.valueOf(Math.pow(10, max - 1)).intValue();
                    int maxVal = Double.valueOf(Math.pow(10, max)).intValue();
                    return PLACEHOLDER.substring(0, min - max) + (minVal + RANDOM.nextInt(maxVal - minVal));
                case "$int":
                    min = Integer.parseInt(definitionParts[1]);
                    max = Integer.parseInt(definitionParts[2]);
                    return min + RANDOM.nextInt(max - min);
                case "$choice":
                    String[] choices = definitionParts[1].split("\\|");
                    return choices[RANDOM.nextInt(choices.length)];
                case "$double":
                    return RANDOM.nextDouble();
            }
        }
        return fieldDefinition;
    }
}
