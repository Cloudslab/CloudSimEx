package org.cloudbus.cloudsim.ex.vm;

import static org.junit.Assert.*;

import org.junit.Test;

public class VMStatusTest {

    @Test
    public void testTransitions() {
        // True
        assertTrue(VMStatus.INITIALISING.isValidNextState(VMStatus.RUNNING));
        assertTrue(VMStatus.INITIALISING.isValidNextState(VMStatus.TERMINATED));
        assertTrue(VMStatus.RUNNING.isValidNextState(VMStatus.TERMINATED));

        // False
        assertFalse(VMStatus.RUNNING.isValidNextState(VMStatus.RUNNING));
        assertFalse(VMStatus.TERMINATED.isValidNextState(VMStatus.INITIALISING));
        assertFalse(VMStatus.TERMINATED.isValidNextState(VMStatus.RUNNING));
    }

}
