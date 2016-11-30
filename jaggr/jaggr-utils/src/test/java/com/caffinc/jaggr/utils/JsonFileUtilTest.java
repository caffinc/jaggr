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
 * Tests the JsonFileUtil utility
 *
 * @author Sriram
 * @since 11/27/2016
 */
public class JsonFileUtilTest {
    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
    private static final Random RANDOM = new Random();
    private static final Gson GSON = new Gson();

    @Test
    public void testGetFileLines() throws Exception {
        Path tempFilePath = Paths.get(TEMP_DIR, "jsontest" + RANDOM.nextInt() + ".json");
        try {
            List<String> lines;
            try (BufferedWriter br = new BufferedWriter(new FileWriter(tempFilePath.toFile()))
            ) {
                for (int i = 0; i < 10; i++)
                    br.write(i + "\n");
            }
            lines = JsonFileUtil.getFileLines(tempFilePath.toString());
            for (int i = 0; i < 10; i++) {
                Assert.assertEquals("Value should match value written to file", String.valueOf(i), lines.get(i));
            }
        } finally {
            Files.delete(tempFilePath);
        }
    }

    @Test
    public void testReadJsonFromFile() throws Exception {
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
            List<Map<String, Object>> jsonLines = JsonFileUtil.readJsonFromFile(tempFilePath.toString());
            for (int i = 0; i < 10; i++) {
                Assert.assertEquals("Value should match value written to file", expectedData.get(i), jsonLines.get(i));
            }
        } finally {
            Files.delete(tempFilePath);
        }
    }

    @Test
    public void testReadJsonFromResource() throws Exception {
        List<Map<String, Object>> result = JsonFileUtil.readJsonFromResource("raw.json");
        Assert.assertEquals("There must be ten lines in the file", 10, result.size());
        for (Map<String, Object> obj : result) {
            Assert.assertEquals("There must be 9 elements in the object", 9, obj.size());
        }
    }
}
