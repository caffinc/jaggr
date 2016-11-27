package com.caffinc.jaggr.core.utils;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the FieldValueExtractor
 *
 * @author Sriram
 * @since 11/26/2016
 */
public class FieldValueExtractorTest {
    private static final Gson gson = new Gson();

    @Test
    public void testSimpleFieldExtraction() throws Exception {
        Map<String, Object> obj = gson.fromJson("{\"_id\" : \"a\"}", HashMap.class);
        Assert.assertEquals("Value extracted should match value in the JSON",
                "a",
                FieldValueExtractor.getValue("_id", obj));
    }

    @Test
    public void testNestedFieldExtraction() throws Exception {
        Map<String, Object> obj = gson.fromJson("{\"l1\" : {\"l2\" : \"a\"}}", HashMap.class);
        Assert.assertEquals("Value extracted should match value in the JSON",
                "a",
                FieldValueExtractor.getValue("l1.l2", obj));
    }

    @Test
    public void testNonexistentFieldExtraction() throws Exception {
        Map<String, Object> obj = gson.fromJson("{\"l1\" : {\"l2\" : \"a\"}}", HashMap.class);
        Assert.assertNull("Non-existent value should be extracted as null from the JSON",
                FieldValueExtractor.getValue("l1.l3", obj));
    }
}
