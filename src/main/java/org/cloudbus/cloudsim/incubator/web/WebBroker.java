package org.cloudbus.cloudsim.incubator.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.incubator.util.CustomLog;
import org.cloudbus.cloudsim.incubator.web.workload.WorkloadGenerator;

/**
 * A broker that takes care of the submission of web sessions to the data
 * centers it handles. The broker submits the cloudlets of the provided web
 * sessions continously over a specified period. Consequently clients must
 * specify the endpoint (in terms of time) of the simulation.
 * 
 * @author nikolay.grozev
 * 
 */
public class WebBroker extends DatacenterBroker {

    // FIXME find a better way to get an unused tag instead of hardcoding 123456
    private static final int TIMER_TAG = 123456;
    private static final int SUBMIT_SESSION_TAG = TIMER_TAG + 1;

    private boolean isTimerRunning = false;
    private final double refreshPeriod;
    private final double lifeLength;

    private Map<Long, ILoadBalancer> loadBalancers = new HashMap<>();
    private Map<Long, List<WorkloadGenerator>> loadBalancersToGenerators = new HashMap<>();

    private List<WebSession> servedSessions = new ArrayList<>();
    private List<WebSession> canceledSessions = new ArrayList<>();
    private List<PresetEvent> presetEvents = new ArrayList<>();

    /**
     * By default CloudSim's brokers use all available datacenters. So we need
     * to enforce only the data centers we want.
     */
    private List<Integer> dataCenterIds;

    /**
     * Creates a new web broker.
     * 
     * @param name
     *            - the name of the broker.
     * @param refreshPeriod
     *            - the period of polling web sessions for new cloudlets.
     * @param lifeLength
     *            - the length of the simulation.
     * @throws Exception
     *             - if something goes wrong. See the documentation of the super
     *             class.
     */
    public WebBroker(final String name, final double refreshPeriod,
	    final double lifeLength) throws Exception {
	this(name, refreshPeriod, lifeLength, null);
    }

    /**
     * Creates a new web broker.
     * 
     * @param name
     *            - the name of the broker.
     * @param refreshPeriod
     *            - the period of polling web sessions for new cloudlets.
     * @param lifeLength
     *            - the length of the simulation.
     * @param dataCenterIds
     *            - the ids of the datacenters this broker operates with. If
     *            null all present data centers are used.
     * 
     * @throws Exception
     *             - if something goes wrong. See the documentation of the super
     *             class.
     */
    public WebBroker(final String name, final double refreshPeriod,
	    final double lifeLength, List<Integer> dataCenterIds) throws Exception {
	super(name);
	this.refreshPeriod = refreshPeriod;
	this.lifeLength = lifeLength;
	this.dataCenterIds = dataCenterIds;
    }

    /**
     * Returns the session that were canceled due to lack of resources to serve
     * them.
     * 
     * @return the session that were canceled due to lack of resources to serve
     *         them.
     */
    public List<WebSession> getCanceledSessions() {
	return canceledSessions;
    }

    /**
     * Returns the sessions that were sucessfully served.
     * 
     * @return the sessions that were sucessfully served.
     */
    public List<WebSession> getServedSessions() {
	return servedSessions;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.DatacenterBroker#processEvent(org.cloudbus.cloudsim
     * .core.SimEvent)
     */
    @Override
    public void processEvent(SimEvent ev) {
	if (!isTimerRunning) {
	    isTimerRunning = true;
	    sendNow(getId(), TIMER_TAG);

	    for (ListIterator<PresetEvent> iter = presetEvents.listIterator(); iter.hasNext();) {
		PresetEvent event = iter.next();
		send(event.getId(), event.getDelay(), event.getTag(), event.getData());
		iter.remove();
	    }
	}

	super.processEvent(ev);
    }

    /**
     * Submits new web sessions to this broker.
     * 
     * @param webSessions
     *            - the new web sessions.
     */
    public void submitSessions(final List<WebSession> webSessions, final long loadBalancerId) {
	List<WebSession> copyWebSessions = new ArrayList<>(webSessions);
	loadBalancers.get(loadBalancerId).assignToServers(
		copyWebSessions.toArray(new WebSession[copyWebSessions.size()]));

	for (ListIterator<WebSession> iter = copyWebSessions.listIterator(); iter.hasNext();) {
	    WebSession session = iter.next();
	    // If the load balancer could not assign it...
	    if (session.getAppVmId() == null || session.getDbVmId() == null) {
		iter.remove();
		canceledSessions.add(session);
		CustomLog.printf(
			"Session could not be served and is canceled. Session id:%d", session.getSessionId());
	    }
	    session.setUserId(getId());
	}

	servedSessions.addAll(copyWebSessions);
    }

    /**
     * Submits new sessions after the specified delay.
     * 
     * @param webSessions
     *            - the list of sessions to submit.
     * @param loadBalancerId
     *            - the id of the load balancer to submit to.
     * @param delay
     *            - the delay to submit after.
     */
    public void submitSessionsAtTime(final List<WebSession> webSessions, final long loadBalancerId, final double delay) {
	Object data = new Object[] { webSessions, loadBalancerId };
	if (isTimerRunning) {
	    send(getId(), delay, SUBMIT_SESSION_TAG, data);
	} else {
	    presetEvents.add(new PresetEvent(getId(), SUBMIT_SESSION_TAG, data, delay));
	}
    }

    /**
     * Adds a new load balancer for handling incoming sessions.
     * 
     * @param balancer
     *            - the balancer to add. Must not be null.
     */
    public void addLoadBalancer(final ILoadBalancer balancer) {
	loadBalancers.put(balancer.getId(), balancer);
	loadBalancersToGenerators.put(balancer.getId(), new ArrayList<WorkloadGenerator>());
    }

    /**
     * Adds a new workload generator for the specified load balancer.
     * 
     * @param workloads
     *            - the workload generators.
     * @param loadBalancerId
     *            - the id of the load balancer. There must such a load balancer
     *            registered before this method is called.
     */
    public void addWorkloadGenerators(final List<WorkloadGenerator> workloads, final long loadBalancerId) {
	loadBalancersToGenerators.get(loadBalancerId).addAll(workloads);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.DatacenterBroker#processOtherEvent(org.cloudbus
     * .cloudsim.core.SimEvent)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void processOtherEvent(SimEvent ev) {
	switch (ev.getTag()) {
	    case TIMER_TAG:
		if (CloudSim.clock() < lifeLength) {
		    send(getId(), refreshPeriod, TIMER_TAG);
		    updateSessions();
		    generateWorkload();
		}
		break;
	    case SUBMIT_SESSION_TAG:
		Object[] data = (Object[]) ev.getData();
		submitSessions((List<WebSession>) data[0], (Long) data[1]);
		break;
	    default:
		super.processOtherEvent(ev);
	}
    }

    private void generateWorkload() {
	double currTime = CloudSim.clock();
	for (Map.Entry<Long, List<WorkloadGenerator>> balancersToWorkloadGens : loadBalancersToGenerators.entrySet()) {
	    long balancerId = balancersToWorkloadGens.getKey();
	    for (WorkloadGenerator gen : balancersToWorkloadGens.getValue()) {
		Map<Double, WebSession> timeToSessions = gen.generateSessions(currTime, refreshPeriod);
		for (Map.Entry<Double, WebSession> sessEntry : timeToSessions.entrySet()) {
		    if (currTime == sessEntry.getKey()) {
			submitSessions(Arrays.asList(sessEntry.getValue()), balancerId);
		    } else {
			submitSessionsAtTime(Arrays.asList(sessEntry.getValue()), balancerId,
				sessEntry.getKey() - currTime);
		    }
		}
	    }
	}
    }

    private void updateSessions() {
	for (WebSession sess : servedSessions) {
	    double currTime = CloudSim.clock();

	    sess.notifyOfTime(currTime);
	    WebCloudlet[] webCloudlets = sess.pollCloudlets(currTime);

	    if (webCloudlets != null) {
		getCloudletList().addAll(Arrays.asList(webCloudlets));
		submitCloudlets();
	    }
	}
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.DatacenterBroker#processCloudletReturn(org.cloudbus
     * .cloudsim.core.SimEvent)
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
	Cloudlet cloudlet = (Cloudlet) ev.getData();
	if (CloudSim.clock() >= lifeLength) {
	    // kill the broker only if it's life length is over/expired
	    super.processCloudletReturn(ev);
	} else {
	    getCloudletReceivedList().add(cloudlet);
	    cloudletsSubmitted--;
	}
    }

    /*
     * (non-Javadoc)
     * 
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
	Log.printLine(getName() + " is starting...");
	schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST, dataCenterIds);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
	setDatacenterIdsList(ev.getData() == null ? CloudSim.getCloudResourceList() : (List<Integer>) ev.getData());
	setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

	for (Integer datacenterId : getDatacenterIdsList()) {
	    sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
	}
    }

    /**
     * CloudSim does not execute events that are fired before the simulation has
     * started. Thus we need to buffer them and then refire when the simulation
     * starts.
     * 
     * @author nikolay.grozev
     * 
     */
    private static class PresetEvent {
	private int id;
	private int tag;
	private Object data;
	private double delay;

	public PresetEvent(int id, int tag, Object data, double delay) {
	    super();
	    this.id = id;
	    this.tag = tag;
	    this.data = data;
	    this.delay = delay;
	}

	public int getId() {
	    return id;
	}

	public int getTag() {
	    return tag;
	}

	public Object getData() {
	    return data;
	}

	public double getDelay() {
	    return delay;
	}
    }
}
