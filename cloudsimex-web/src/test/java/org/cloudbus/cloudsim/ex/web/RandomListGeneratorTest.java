package org.cloudbus.cloudsim.ex.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.junit.Test;

public strictfp class RandomListGeneratorTest {

    @Test
    public strictfp void test() {
        String test1 = "Test1";
        double testFreq1 = 11;

        String test2 = "Test2";
        double testFreq2 = 2;

        String test3 = "Test3";
        double testFreq3 = 50;

        String test4 = "Test4";
        double testFreq4 = 0.1;

        double sumOfFreqs = testFreq1 + testFreq2 + testFreq3 + testFreq4;

        Map<String, Double> values = new HashMap<>();
        values.put(test1, testFreq1);
        values.put(test2, testFreq2);
        values.put(test3, testFreq3);
        values.put(test4, testFreq4);

        // Instance to test
        RandomListGenerator<String> gen = new RandomListGenerator<String>(values, TestUtil.SEED);

        // Test initial peeking works fine
        assertNotNull(gen.peek());
        assertEquals(gen.peek(), gen.peek());
        assertEquals(gen.peek(), gen.poll());

        // Count the occurrences
        Map<String, Integer> counts = new HashMap<>();
        int numTests = 10_000;
        for (int i = 0; i < numTests; i++) {
            String s = gen.poll();
            if (!counts.containsKey(s)) {
                counts.put(s, 0);
            }
            counts.put(s, counts.get(s) + 1);

            // Subsequent peeks should return the same
            assertEquals(gen.peek(), gen.poll());
            assertEquals(gen.peek(), gen.poll());

            // This generator is never empty
            assertFalse(gen.isEmpty());
        }

        double delta = 0.01;
        assertEquals(testFreq1 / sumOfFreqs, counts.get(test1) / (double) numTests, delta);
        assertEquals(testFreq2 / sumOfFreqs, counts.get(test2) / (double) numTests, delta);
        assertEquals(testFreq3 / sumOfFreqs, counts.get(test3) / (double) numTests, delta);
        assertEquals(testFreq4 / sumOfFreqs, counts.get(test4) / (double) numTests, delta);
    }

}
