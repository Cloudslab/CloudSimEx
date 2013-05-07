package org.cloudbus.cloudsim.ex.web.workload.sessions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.disk.DataItem;
import org.cloudbus.cloudsim.ex.web.CompositeGenerator;
import org.cloudbus.cloudsim.ex.web.IGenerator;
import org.cloudbus.cloudsim.ex.web.StatGenerator;
import org.cloudbus.cloudsim.ex.web.WebCloudlet;
import org.cloudbus.cloudsim.ex.web.WebSession;

public class StatSessionGenerator implements ISessionGenerator {

    private final Map<String, List<Double>> asSessionParams;
    private final Map<String, List<Double>> dbSessionParams;
    private final int userId;
    private final double idealLength;
    private final DataItem data;

    public StatSessionGenerator(final Map<String, List<Double>> asSessionParams,
	    final Map<String, List<Double>> dbSessionParams,
	    final int userId, final DataItem data, final int step) {
	super();
	this.asSessionParams = asSessionParams;
	this.dbSessionParams = dbSessionParams;
	this.userId = userId;
	this.data = data;

	this.idealLength = Math.max(Collections.max(asSessionParams.get("Time")),
		Collections.max(dbSessionParams.get("Time"))) + step;
    }

    @Override
    public WebSession generateSessionAt(final double time) {
	final IGenerator<? extends WebCloudlet> appServerCloudLets = new StatGenerator(
		GeneratorsUtil.toGenerators(asSessionParams), data);
	final IGenerator<? extends Collection<? extends WebCloudlet>> dbServerCloudLets = new CompositeGenerator<>(
		new StatGenerator(GeneratorsUtil.toGenerators(dbSessionParams), data));

	int cloudletsNumber = asSessionParams.get(asSessionParams.keySet().toArray()[0]).size();
	return new WebSession(appServerCloudLets,
		dbServerCloudLets,
		userId,
		cloudletsNumber,
		time + idealLength);
    }
}
