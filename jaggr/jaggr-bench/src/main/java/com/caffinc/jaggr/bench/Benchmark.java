package com.caffinc.jaggr.bench;

import com.caffinc.jaggr.core.Aggregation;
import com.caffinc.jaggr.core.AggregationBuilder;
import com.caffinc.jaggr.core.operations.CollectSetOperation;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
//        generateDocuments(BENCHMARK_COLLECTION, 1000000, getFieldDefinitions());
        final Aggregation aggregation = new AggregationBuilder()
                .setGroupBy("name")
                .addOperation("groupedIds", new CollectSetOperation("_id"))
                .getAggregation();
        long startTime;
        LOG.info("Computing read time");
        startTime = System.currentTimeMillis();
        Iterator<Map<String, Object>> dbObjectIterator = new Iterator<Map<String, Object>>() {
            private Iterator<DBObject> objectIterator = BENCHMARK_COLLECTION.find().iterator();

            @Override
            public boolean hasNext() {
                return objectIterator.hasNext();
            }

            @Override
            public Map<String, Object> next() {
                if (objectIterator.hasNext())
                    return objectIterator.next().toMap();
                else
                    return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        List<Map<String, Object>> inMemList = new ArrayList<>();
        while (dbObjectIterator.hasNext()) {
            inMemList.add(dbObjectIterator.next());
        }
        LOG.info("Read time: {}ms {} docs", (System.currentTimeMillis() - startTime), inMemList.size());

        LOG.info("Starting aggregation");
        startTime = System.currentTimeMillis();
        List<Map<String, Object>> result = aggregation.aggregate(new Iterator<Map<String, Object>>() {
            private Iterator<DBObject> objectIterator = BENCHMARK_COLLECTION.find().iterator();

            @Override
            public boolean hasNext() {
                return objectIterator.hasNext();
            }

            @Override
            public Map<String, Object> next() {
                if (objectIterator.hasNext())
                    return objectIterator.next().toMap();
                else
                    return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        });
        LOG.info("Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());

        startTime = System.currentTimeMillis();
        LOG.info("Starting native aggregation");
        List<Map<String, Object>> mongoResults = new ArrayList<>();
        for (DBObject dbObject : BENCHMARK_COLLECTION.aggregate(
                Arrays.asList(
                        new BasicDBObject("$group", new BasicDBObject("_id", "$name").append("groupedIds", new BasicDBObject("$addToSet", "$_id")))
                )
        ).results()) {
            mongoResults.add(dbObject.toMap());
        }
        LOG.info("Mongo Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), mongoResults.size());


        LOG.info("Starting aggregation");
        startTime = System.currentTimeMillis();
        result = aggregation.aggregate(inMemList);
        LOG.info("In-memory Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());

        int docCount = inMemList.size();
        int threadCount = 10;
        final int limit = Double.valueOf(Math.ceil(((double) docCount) / threadCount)).intValue();
        final AtomicInteger counter = new AtomicInteger(0);
        LOG.info("Starting multithreaded aggregation");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            final int batchId = i;
            new Thread() {
                @Override
                public void run() {
                    runAggregation(counter, aggregation, limit, batchId);
                }
            }.start();
        }
        while(counter.get() < docCount) {
            Thread.sleep(1);
        }
        LOG.info("Multi-threaded Aggregation time: {}ms {} docs", (System.currentTimeMillis() - startTime), result.size());
    }

    private static void runAggregation(final AtomicInteger counter, final Aggregation aggregation, final int limit, final int batchId) {
        LOG.info("Starting aggregation");
        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> result = aggregation.aggregate(new Iterator<Map<String, Object>>() {
            private Iterator<DBObject> objectIterator = BENCHMARK_COLLECTION.find().skip(limit * batchId).limit(limit).iterator();

            @Override
            public boolean hasNext() {
                return objectIterator.hasNext();
            }

            @Override
            public Map<String, Object> next() {
                counter.incrementAndGet();
                if (objectIterator.hasNext())
                    return objectIterator.next().toMap();
                else
                    return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
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
