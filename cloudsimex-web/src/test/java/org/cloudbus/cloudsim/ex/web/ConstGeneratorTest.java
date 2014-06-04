package org.cloudbus.cloudsim.ex.web;

import static org.junit.Assert.*;

import org.junit.Test;

public class ConstGeneratorTest {

    @Test
    public void testConstGeneratorWithValue() {
        String value = "Test";
        ConstGenerator<String> gen = new ConstGenerator<>(value);
        for (int i = 0; i < 10; i++) {
            assertEquals(value, gen.peek());
            assertEquals(value, gen.poll());
            assertFalse(value, gen.isEmpty());

            // Should have no effect.
            gen.notifyOfTime(10);
        }
    }

}
