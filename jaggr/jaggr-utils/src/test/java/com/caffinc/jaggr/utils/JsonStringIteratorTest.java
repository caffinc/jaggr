package com.caffinc.jaggr.utils;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Tests for the JsonStringIterator
 *
 * @author Sriram
 * @since 11/27/2016
 */
public class JsonStringIteratorTest {
    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
    private static final Random RANDOM = new Random();
    private static final Gson GSON = new Gson();

    @Test
    public void testJsonFileIterator() throws Exception {
        Path tempFilePath = Paths.get(TEMP_DIR, "jsontest" + RANDOM.nextInt() + ".json");
        try {
            List<Map<String, Object>> expectedData = new ArrayList<>();
            try (BufferedWriter br = new BufferedWriter(new FileWriter(tempFilePath.toFile()))
            ) {
                for (int i = 0; i < 10; i++) {
                    Map<String, Object> json = new HashMap<>();
                    json.put("_id", (double) i);
                    json.put("val", RANDOM.nextDouble());
                    expectedData.add(json);
                    br.write(GSON.toJson(json) + "\n");
                }
            }
            try (JsonStringIterator jsonStringIterator = new JsonStringIterator(tempFilePath.toString())) {
                for (Map<String, Object> expected : expectedData) {
                    Map<String, Object> actual = jsonStringIterator.next();
                    Assert.assertEquals("Value should match value written to file", expected, actual);
                }
            }
        } finally {
            Files.delete(tempFilePath);
        }
    }
}
