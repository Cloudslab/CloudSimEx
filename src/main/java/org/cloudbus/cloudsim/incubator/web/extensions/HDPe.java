package org.cloudbus.cloudsim.incubator.web.extensions;

import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.provisioners.PeProvisioner;

/**
 * 
 * @author nikolay.grozev
 *
 */
public class HDPe extends Pe {

    public HDPe(PeProvisioner peProvisioner) {
	super(Id.pollId(HDPe.class), peProvisioner);
    }

}
