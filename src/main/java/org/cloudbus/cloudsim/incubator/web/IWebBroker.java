package org.cloudbus.cloudsim.incubator.web;

import java.util.List;

import org.cloudbus.cloudsim.Vm;

/**
 * Represents common functionalities of the web broker. Using interfaces does
 * not follow the style if CloudSim, but we need this one to be able to write
 * mock tests.
 * 
 * @author nikolay.grozev
 * 
 */
public interface IWebBroker {

    public int getId();
    
    public <T extends Vm> List<T> getVmList();
}
