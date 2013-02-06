package org.cloudbus.cloudsim.incubator.web;

import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.incubator.disk.DataItem;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class DBCompositeCloudlet {

    private int sessionId;

    private final Map<DataItem, Boolean> modifyData = new LinkedHashMap<>();

    // private final Map<DataItem, Long>

    // private final List<WebCloudlet> dbCloudlets = new ArrayList<>();

}
