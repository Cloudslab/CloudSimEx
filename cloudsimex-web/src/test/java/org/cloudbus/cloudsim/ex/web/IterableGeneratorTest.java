package org.cloudbus.cloudsim.ex.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.ex.web.IterableGenerator;
import org.junit.Test;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class IterableGeneratorTest {

    @Test
    public void testEmptyGenerator() {
        IterableGenerator<Integer> generator = new IterableGenerator<>();
        assertTrue(generator.isEmpty());
        assertNull(generator.peek());
        assertNull(generator.poll());
    }

    @Test
    public void testWhatIsInGetsOut() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4);
        IterableGenerator<Integer> generator = new IterableGenerator<>(data);

        int i = 0;
        while (!generator.isEmpty()) {
            Integer datum = generator.poll();
            assertEquals(data.get(i), datum);
            i++;
        }

        assertEquals(data.size(), i);
    }

    @Test
    public void testPeekPoll() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4);
        IterableGenerator<Integer> generator = new IterableGenerator<>(data);

        for (int i = 0; i < data.size(); i++) {
            Object peeked = generator.peek();
            Object peekedAgain = generator.peek();
            Object polled = generator.poll();
            assertNotNull(peeked);
            assertTrue(peeked == peekedAgain);
            assertTrue(peeked == polled);
        }
    }

}
