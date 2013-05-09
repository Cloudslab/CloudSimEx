package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.util.Id;

public class DataSource extends SimEntity {

    public Double cost;

    public DataSource(String name, Double cost) {
	super(name);
	setId(Id.pollId(DataSource.class));

	this.cost = cost;
    }

    @Override
    public void startEntity() {
	// TODO Auto-generated method stub

    }

    @Override
    public void processEvent(SimEvent ev) {
	// TODO Auto-generated method stub

    }

    @Override
    public void shutdownEntity() {
	// TODO Auto-generated method stub

    }
}
