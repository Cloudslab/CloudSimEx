package org.cloudbus.cloudsim.ex.web;

import static org.cloudbus.cloudsim.ex.util.helpers.TestUtil.createSeededGaussian;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.disk.DataItem;
import org.cloudbus.cloudsim.ex.disk.HddCloudlet;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.uncommons.maths.number.NumberGenerator;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class WebSessionTest {

    private static final int GEN_RAM_MEAN = 200;
    private static final int GEN_RAM_STDEV = 10;
    private NumberGenerator<Double> genRAM_AS;
    private NumberGenerator<Double> genRAM_DB;

    private static final int GEN_CPU_MEAN = 25;
    private static final int GEN_CPU_STDEV = 2;
    private NumberGenerator<Double> genCPU_AS;
    private NumberGenerator<Double> genCPU_DB;

    private Map<String, NumberGenerator<Double>> testGeneratorsAS;
    private Map<String, NumberGenerator<Double>> testGeneratorsDB1;
    private Map<String, NumberGenerator<Double>> testGeneratorsDB2;

    // Generate cloudlets statistically on every notification
    private IGenerator<TestWebCloudlet> statGeneratorAS;
    private IGenerator<TestWebCloudlet> statGeneratorDB1;
    private IGenerator<TestWebCloudlet> statGeneratorDB2;

    /** Generate cloudlets once every three notifications. */
    private IGenerator<TestWebCloudlet> onceInThreeGeneratorDB3;

    /** Creates 1 AS and 1 DB cloudlet for every notification. */
    private WebSession singleDataItemStatSession;
    /** Creates 1 AS and 2 DB cloudlet for every notification. */
    private WebSession twoDataItemsStatSession;
    /**
     * Creates 1 AS on every notification. Creates 2 DB cloudlets every third
     * notification, and 1 DB cloudlet otherwise.
     */
    private WebSession twoDataItemsCasuallySession;

    private final int numCloudletsInSession2 = 10;

    private static final DataItem data1 = new DataItem(65);
    private static final DataItem data2 = new DataItem(65);

    @Before
    public void setUp() throws SecurityException, IOException {
        CustomLog.configLogger(TestUtil.LOG_PROPS);

        IDBBalancer dummyBalancer = new IDBBalancer() {
            @Override
            public void setVms(final List<HddVm> vms) {
            }

            @Override
            public List<HddVm> getVMs() {
                return null;
            }

            @Override
            public void allocateToServer(final HddCloudlet cloudlet) {
                cloudlet.setVmId(Id.pollId(Vm.class));
            }
        };

        genRAM_AS = createSeededGaussian(GEN_RAM_MEAN, GEN_RAM_STDEV);
        genRAM_DB = createSeededGaussian(GEN_RAM_MEAN, GEN_RAM_STDEV);

        genCPU_AS = createSeededGaussian(GEN_CPU_MEAN, GEN_CPU_STDEV);
        genCPU_DB = createSeededGaussian(GEN_CPU_MEAN, GEN_CPU_STDEV);

        testGeneratorsAS = new HashMap<>();
        testGeneratorsAS.put(StatGenerator.CLOUDLET_LENGTH, genCPU_AS);
        testGeneratorsAS.put(StatGenerator.CLOUDLET_RAM, genRAM_AS);

        testGeneratorsDB1 = new HashMap<>();
        testGeneratorsDB1.put(StatGenerator.CLOUDLET_LENGTH, genCPU_DB);
        testGeneratorsDB1.put(StatGenerator.CLOUDLET_RAM, genRAM_DB);

        testGeneratorsDB2 = new HashMap<>();
        testGeneratorsDB2.putAll(testGeneratorsDB1);

        statGeneratorAS = new TestStatGenerator(testGeneratorsAS, 1, null);
        statGeneratorDB1 = new TestStatGenerator(testGeneratorsDB1, 1, data1);
        statGeneratorDB2 = new TestStatGenerator(testGeneratorsDB1, 1, data2);
        onceInThreeGeneratorDB3 = new OnceInThreeGenerator(data1);

        singleDataItemStatSession = new WebSession(statGeneratorAS, new CompositeGenerator<>(statGeneratorDB1), 1, -1,
                100);
        singleDataItemStatSession.setAppVmId(Id.pollId(Vm.class));
        singleDataItemStatSession.setDbBalancer(dummyBalancer);

        twoDataItemsStatSession = new WebSession(statGeneratorAS, new CompositeGenerator<>(statGeneratorDB1,
                statGeneratorDB2), 1, numCloudletsInSession2, 100);
        twoDataItemsStatSession.setAppVmId(Id.pollId(Vm.class));
        twoDataItemsStatSession.setDbBalancer(dummyBalancer);

        twoDataItemsCasuallySession = new WebSession(statGeneratorAS, new CompositeGenerator<>(onceInThreeGeneratorDB3,
                statGeneratorDB2), 1, numCloudletsInSession2, 100);
        twoDataItemsCasuallySession.setAppVmId(Id.pollId(Vm.class));
        twoDataItemsCasuallySession.setDbBalancer(dummyBalancer);

    }

    @Test
    public void testBeyoundCapacitySingleDataItemAccessed() {
        int time = 0;

        for (int i = 0; i < numCloudletsInSession2; i++) {
            twoDataItemsStatSession.notifyOfTime(time++);
            WebSession.StepCloudlets currCloudLets = twoDataItemsStatSession.pollCloudlets(time++);
            assertNotNull(currCloudLets);
            ((TestWebCloudlet) currCloudLets.asCloudlet).setFinished(true);
            assertEquals(currCloudLets.dbCloudlets.size(), 2);
            ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);
            ((TestWebCloudlet) currCloudLets.dbCloudlets.get(1)).setFinished(true);
        }
        twoDataItemsStatSession.notifyOfTime(time++);
        assertNull(twoDataItemsStatSession.pollCloudlets(time++));
    }

    @Test
    public void testBeyoundCapacityTwoDataItemsAccessed() {
        int time = 0;

        for (int i = 0; i < numCloudletsInSession2; i++) {
            twoDataItemsStatSession.notifyOfTime(time++);
            WebSession.StepCloudlets currCloudLets = twoDataItemsStatSession.pollCloudlets(time++);
            assertNotNull(currCloudLets);
            ((TestWebCloudlet) currCloudLets.asCloudlet).setFinished(true);
            assertEquals(currCloudLets.dbCloudlets.size(), 2);
            ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);
            ((TestWebCloudlet) currCloudLets.dbCloudlets.get(1)).setFinished(true);
        }
        twoDataItemsStatSession.notifyOfTime(time++);
        assertNull(twoDataItemsStatSession.pollCloudlets(time++));
    }

    @Test
    public void testPollingEmptySession() {
        int currTime = 0;
        assertNull(singleDataItemStatSession.pollCloudlets(currTime));
    }

    @Test
    public void testSynchPollingSingleDataItemAccessed() {
        int currTime = 0;

        singleDataItemStatSession.notifyOfTime(currTime++);
        WebSession.StepCloudlets currCloudLets = singleDataItemStatSession.pollCloudlets(currTime++);
        assertNotNull(currCloudLets);
        assertNotNull(currCloudLets.asCloudlet);
        assertEquals(currCloudLets.dbCloudlets.size(), 1);
        assertNotNull(currCloudLets.dbCloudlets.get(0));

        singleDataItemStatSession.notifyOfTime(currTime++);

        // These cloudlets are not finished - no new ones should be available
        assertNull(singleDataItemStatSession.pollCloudlets(currTime++));

        // Finish one of the cloudlets
        ((TestWebCloudlet) currCloudLets.asCloudlet).setFinished(true);

        // One of the cloudlets is not finished - no new ones should be
        // available
        assertNull(singleDataItemStatSession.pollCloudlets(currTime++));

        // Finish the other of the cloudlets
        ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);

        // Now we can poll again
        currCloudLets = singleDataItemStatSession.pollCloudlets(currTime++);
        assertNotNull(currCloudLets);
        assertNotNull(currCloudLets.asCloudlet);
        assertEquals(currCloudLets.dbCloudlets.size(), 1);
        assertNotNull(currCloudLets.dbCloudlets.get(0));
    }

    @Test
    public void testSynchPollingTwoDataItemAccessed() {
        int currTime = 0;

        twoDataItemsStatSession.notifyOfTime(currTime++);
        WebSession.StepCloudlets currCloudLets = twoDataItemsStatSession.pollCloudlets(currTime++);
        assertNotNull(currCloudLets);
        assertNotNull(currCloudLets.asCloudlet);
        assertEquals(currCloudLets.dbCloudlets.size(), 2);
        assertNotNull(currCloudLets.dbCloudlets.get(0));
        assertNotNull(currCloudLets.dbCloudlets.get(1));

        twoDataItemsStatSession.notifyOfTime(currTime++);

        // These cloudlets are not finished - no new ones should be available
        assertNull(twoDataItemsStatSession.pollCloudlets(currTime++));

        // Finish one of the DB cloudlets
        ((TestWebCloudlet) currCloudLets.dbCloudlets.get(1)).setFinished(true);

        // One of the DB cloudlets and the AS cloudlet are not finished - no new
        // ones should be available
        assertNull(twoDataItemsStatSession.pollCloudlets(currTime++));

        // Finish the AS cloudlets
        ((TestWebCloudlet) currCloudLets.asCloudlet).setFinished(true);

        // One of the DB cloudlets is not finished - no new
        // ones should be available
        assertNull(twoDataItemsStatSession.pollCloudlets(currTime++));

        // Finish the last cloudlets
        ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);

        // Now we can poll again
        currCloudLets = twoDataItemsStatSession.pollCloudlets(currTime++);
        assertNotNull(currCloudLets);
        assertNotNull(currCloudLets.asCloudlet);
        assertEquals(currCloudLets.dbCloudlets.size(), 2);
        assertNotNull(currCloudLets.dbCloudlets.get(0));
        assertNotNull(currCloudLets.dbCloudlets.get(1));
    }

    @Test
    public void testExhaustivePollingSingleDataItem() {
        int currTime = 0;

        for (int i = 0; i < 10; i++) {
            singleDataItemStatSession.notifyOfTime(currTime++);
            WebSession.StepCloudlets currCloudLets = singleDataItemStatSession.pollCloudlets(currTime++);
            assertNotNull(currCloudLets);
            assertNotNull(currCloudLets.asCloudlet);
            assertEquals(currCloudLets.dbCloudlets.size(), 1);
            assertNotNull(currCloudLets.dbCloudlets.get(0));

            // Assert we have runnable cloudlets
            assertFalse(currCloudLets.asCloudlet.isFinished());
            assertFalse(currCloudLets.dbCloudlets.get(0).isFinished());

            // Finish the cloudlets
            ((TestWebCloudlet) currCloudLets.asCloudlet).setFinished(true);
            ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);
        }

        assertNull(singleDataItemStatSession.pollCloudlets(currTime++));
    }

    @Test
    public void testExhaustivePollingTwoDataItems() {
        int currTime = 0;

        for (int i = 0; i < 10; i++) {
            twoDataItemsStatSession.notifyOfTime(currTime++);
            WebSession.StepCloudlets currCloudLets = twoDataItemsStatSession.pollCloudlets(currTime++);
            assertNotNull(currCloudLets);
            assertNotNull(currCloudLets.asCloudlet);
            assertEquals(currCloudLets.dbCloudlets.size(), 2);
            assertNotNull(currCloudLets.dbCloudlets.get(0));
            assertNotNull(currCloudLets.dbCloudlets.get(1));

            // Assert we have runnable cloudlets
            assertFalse(currCloudLets.asCloudlet.isFinished());
            assertFalse(currCloudLets.dbCloudlets.get(0).isFinished());
            assertFalse(currCloudLets.dbCloudlets.get(1).isFinished());

            // Finish the cloudlets
            ((TestWebCloudlet) currCloudLets.asCloudlet).setFinished(true);
            ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);
            ((TestWebCloudlet) currCloudLets.dbCloudlets.get(1)).setFinished(true);
        }

        assertNull(twoDataItemsStatSession.pollCloudlets(currTime++));
    }

    @Test
    public void testNonSynchDBCloudlets() {
        int currTime = 0;

        for (int i = 0; i < 10; i++) {
            twoDataItemsCasuallySession.notifyOfTime(currTime++);
            WebSession.StepCloudlets currCloudLets = twoDataItemsCasuallySession.pollCloudlets(currTime++);
            assertNotNull(currCloudLets);
            assertNotNull(currCloudLets.asCloudlet);

            // Assert we have runnable cloudlets
            assertFalse(currCloudLets.asCloudlet.isFinished());

            if (i % 3 == 0) {
                assertEquals(currCloudLets.dbCloudlets.size(), 2);
                assertNotNull(currCloudLets.dbCloudlets.get(0));
                assertNotNull(currCloudLets.dbCloudlets.get(1));

                // Assert we have runnable cloudlets
                assertFalse(currCloudLets.dbCloudlets.get(0).isFinished());
                assertFalse(currCloudLets.dbCloudlets.get(1).isFinished());

                // Finish the first db cloudlets
                ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);
                ((TestWebCloudlet) currCloudLets.dbCloudlets.get(1)).setFinished(true);
            } else {
                assertEquals(currCloudLets.dbCloudlets.size(), 1);
                assertNotNull(currCloudLets.dbCloudlets.get(0));

                // Assert we have runnable cloudlets
                assertFalse(currCloudLets.dbCloudlets.get(0).isFinished());

                // Finish the cloudlets
                ((TestWebCloudlet) currCloudLets.dbCloudlets.get(0)).setFinished(true);
            }

            // Finish the cloudlets
            ((TestWebCloudlet) currCloudLets.asCloudlet).setFinished(true);
        }

        assertNull(twoDataItemsCasuallySession.pollCloudlets(currTime++));

    }

    /**
     * A cloudlet that we can explicitly set as finished for testing purposes.
     * 
     * @author nikolay.grozev
     * 
     */
    private static class TestWebCloudlet extends WebCloudlet {
        private boolean finished;

        public TestWebCloudlet(final double idealStartTime, final long cloudletLength, final long cloudletIOLength,
                final int ram, final int userId, final DataItem data) {
            super(idealStartTime, cloudletLength, cloudletIOLength, ram, userId, false, data);
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        public void setFinished(final boolean finished) {
            this.finished = finished;
        }
    }

    private static class TestStatGenerator extends BaseStatGenerator<TestWebCloudlet> {
        int userId;

        public TestStatGenerator(final Map<String, NumberGenerator<Double>> randomGenerators, final int userId,
                final DataItem data) {
            super(randomGenerators, data);
            this.userId = userId;
        }

        @Override
        protected TestWebCloudlet create(final double idealStartTime) {
            long cpuLen = generateNumericValue(CLOUDLET_LENGTH).longValue();
            int ram = generateNumericValue(CLOUDLET_RAM).intValue();
            int ioLen = generateNumericValue(CLOUDLET_LENGTH).intValue();

            return new TestWebCloudlet(idealStartTime, cpuLen, ioLen, ram, userId, super.getData());
        }
    }

    /**
     * A generator that creates new entities once every 3 calls
     * 
     * @author nikolay.grozev
     * 
     */
    private static class OnceInThreeGenerator implements IGenerator<TestWebCloudlet> {

        private final LinkedList<Double> idealStartUpTimes = new LinkedList<>();
        private TestWebCloudlet peeked;
        private int i = 0;
        private final DataItem data;

        public OnceInThreeGenerator(final DataItem data) {
            this.data = data;
        }

        private TestWebCloudlet create(final Double time) {
            return new TestWebCloudlet(time, 1, 1, 1, 1, data);
        }

        @Override
        public TestWebCloudlet peek() {
            if (peeked == null && !idealStartUpTimes.isEmpty()) {
                peeked = create(idealStartUpTimes.poll());
            }
            return peeked;
        }

        @Override
        public TestWebCloudlet poll() {
            TestWebCloudlet result = peeked;
            if (peeked != null) {
                peeked = null;
            } else if (!idealStartUpTimes.isEmpty()) {
                result = create(idealStartUpTimes.poll());
            }
            return result;
        }

        @Override
        public boolean isEmpty() {
            return peek() == null;
        }

        @Override
        public void notifyOfTime(final double time) {
            if (i++ % 3 == 0) {
                idealStartUpTimes.offer(time);
            }
        }

    }

}
