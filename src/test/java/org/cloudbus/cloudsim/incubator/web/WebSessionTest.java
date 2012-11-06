package org.cloudbus.cloudsim.incubator.web;

import static org.cloudbus.cloudsim.incubator.util.helpers.TestUtil.createSeededGaussian;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.incubator.util.Id;
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
    private Map<String, NumberGenerator<Double>> testGeneratorsDB;

    private BaseStatGenerator<TestWebCloudlet> statGeneratorAS;
    private BaseStatGenerator<TestWebCloudlet> statGeneratorDB;

    private WebSession session;

    @Before
    public void setUp() {

	genRAM_AS = createSeededGaussian(GEN_RAM_MEAN, GEN_RAM_STDEV);
	genRAM_DB = createSeededGaussian(GEN_RAM_MEAN, GEN_RAM_STDEV);

	genCPU_AS = createSeededGaussian(GEN_CPU_MEAN, GEN_CPU_STDEV);
	genCPU_DB = createSeededGaussian(GEN_CPU_MEAN, GEN_CPU_STDEV);

	testGeneratorsAS = new HashMap<>();
	testGeneratorsAS.put(StatGenerator.CLOUDLET_LENGTH, genCPU_AS);
	testGeneratorsAS.put(StatGenerator.CLOUDLET_RAM, genRAM_AS);

	testGeneratorsDB = new HashMap<>();
	testGeneratorsDB.put(StatGenerator.CLOUDLET_LENGTH, genCPU_DB);
	testGeneratorsDB.put(StatGenerator.CLOUDLET_RAM, genRAM_DB);

	statGeneratorAS = new TestStatGenerator(testGeneratorsAS, 1);
	statGeneratorDB = new TestStatGenerator(testGeneratorsDB, 1);

	session = new WebSession(statGeneratorAS, statGeneratorDB, 1, -1, 100);
	session.setAppVmId(Id.pollId(Vm.class));
	session.setDbVmId(Id.pollId(Vm.class));
    }

    @Test
    public void testBeyoundCapacity() {
	int time = 0;
	int numCloudlets = 10;
	WebSession newSession = new WebSession(statGeneratorAS, statGeneratorDB, 1, numCloudlets, 100);
	newSession.setAppVmId(Id.pollId(Vm.class));
	newSession.setDbVmId(Id.pollId(Vm.class));

	for (int i = 0; i < numCloudlets; i++) {
	    newSession.notifyOfTime(time++);
	    WebCloudlet[] currCloudLets = newSession.pollCloudlets(time++);
	    assertNotNull(currCloudLets);
	    ((TestWebCloudlet) currCloudLets[0]).setFinished(true);
	    ((TestWebCloudlet) currCloudLets[1]).setFinished(true);
	}
	newSession.notifyOfTime(time++);
	assertNull(newSession.pollCloudlets(time++));
    }

    @Test
    public void testPollingEmptySession() {
	int currTime = 0;
	assertNull(session.pollCloudlets(currTime));
    }

    @Test
    public void testSynchPolling() {
	int currTime = 0;

	session.notifyOfTime(currTime++);
	WebCloudlet[] currCloudLets = session.pollCloudlets(currTime++);
	assertNotNull(currCloudLets);
	assertNotNull(currCloudLets[0]);
	assertNotNull(currCloudLets[1]);

	session.notifyOfTime(currTime++);

	// These cloudlets are not finished - no new ones should be available
	assertNull(session.pollCloudlets(currTime++));

	// Finish one of the cloudlets
	((TestWebCloudlet) currCloudLets[0]).setFinished(true);

	// One of the cloudlets is not finished - no new ones should be
	// available
	assertNull(session.pollCloudlets(currTime++));

	// Finish the other of the cloudlets
	((TestWebCloudlet) currCloudLets[1]).setFinished(true);

	// Now we can poll again
	currCloudLets = session.pollCloudlets(currTime++);
	assertNotNull(currCloudLets);
	assertNotNull(currCloudLets[0]);
	assertNotNull(currCloudLets[1]);
    }

    @Test
    public void testExhaustivePolling() {
	int currTime = 0;

	for (int i = 0; i < 10; i++) {
	    session.notifyOfTime(currTime++);
	    WebCloudlet[] currCloudLets = session.pollCloudlets(currTime++);
	    assertNotNull(currCloudLets);
	    assertNotNull(currCloudLets[0]);
	    assertNotNull(currCloudLets[1]);

	    // Assert we have runnable cloudlets
	    assertFalse(currCloudLets[0].isFinished());
	    assertFalse(currCloudLets[1].isFinished());

	    // Finish the cloudlets
	    ((TestWebCloudlet) currCloudLets[0]).setFinished(true);
	    ((TestWebCloudlet) currCloudLets[1]).setFinished(true);
	}

	assertNull(session.pollCloudlets(currTime++));
    }

    
    /**
     * A cloudlet that we can explicitly set as finished for testing purposes.
     * 
     * @author nikolay.grozev
     * 
     */
    private static class TestWebCloudlet extends WebCloudlet {
	private boolean finished;

	public TestWebCloudlet(double idealStartTime, long cloudletLength, long cloudletIOLength, int ram, int userId) {
	    super(idealStartTime, cloudletLength, cloudletIOLength, ram, userId);
	}

	@Override
	public boolean isFinished() {
	    return finished;
	}

	public void setFinished(boolean finished) {
	    this.finished = finished;
	}
    }

    private static class TestStatGenerator extends BaseStatGenerator<TestWebCloudlet> {
	int userId;

	public TestStatGenerator(Map<String, NumberGenerator<Double>> randomGenerators, int userId) {
	    super(randomGenerators);
	    this.userId = userId;
	}

	@Override
	protected TestWebCloudlet create(double idealStartTime) {
	    long cpuLen = generateValue(CLOUDLET_LENGTH).longValue();
	    int ram = generateValue(CLOUDLET_RAM).intValue();
	    int ioLen = generateValue(CLOUDLET_LENGTH).intValue();

	    return new TestWebCloudlet(idealStartTime, cpuLen, ioLen, ram, userId);
	}
    }

}
