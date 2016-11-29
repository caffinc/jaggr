package com.caffinc.jaggr.utils;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Tests for the JsonIterator
 *
 * @author Sriram
 * @since 11/28/2016
 */
public class JsonIteratorTest {
    @Test
    public void testIterator() {
        List<String> stringList = Arrays.asList(
                "{\"i\": 0}",
                "{\"i\": 1}",
                "{\"i\": 2}",
                "{\"i\": 3}",
                "{\"i\": 4}",
                "{\"i\": 5}",
                "{\"i\": 6}",
                "{\"i\": 7}",
                "{\"i\": 8}",
                "{\"i\": 9}"
        );
        final JsonIterator jsonIterator = new JsonIterator<String>(stringList.iterator()) {
            private Gson gson = new Gson();

            @Override
            public Map<String, Object> toJson(String element) {
                return gson.fromJson(element, HashMap.class);
            }
        };
        Iterable<Map<String, Object>> iterable = new Iterable<Map<String, Object>>() {
            @Override
            public Iterator<Map<String, Object>> iterator() {
                return jsonIterator;
            }
        };
        int size = 0;
        for (Map<String, Object> obj : iterable) {
            Assert.assertEquals("Value must match the input", (double) size, obj.get("i"));
            size++;
        }
        Assert.assertEquals("Number of values should match input", stringList.size(), size);
    }
}
