package org.cloudbus.cloudsim.ex.util;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.ex.util.Id;
import org.junit.Test;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class IdTest {
    @Test
    public void testPollId() {
        // Test generation of ids for preset entities
        Set<Integer> cloudletIds = new HashSet<>();
        cloudletIds.add(Id.pollId(Cloudlet.class));
        cloudletIds.add(Id.pollId(Cloudlet.class));
        cloudletIds.add(Id.pollId(CloudLetA.class));
        cloudletIds.add(Id.pollId(CloudLetA.class));
        cloudletIds.add(Id.pollId(CloudLetB.class));
        assertEquals(5, cloudletIds.size());

        // Test generation of ids for miscellaneous entities
        Set<Integer> ids = new HashSet<>();
        ids.add(Id.pollId(X.class));
        ids.add(Id.pollId(Y.class));
        ids.add(Id.pollId(Object.class));
        ids.add(Id.pollId(X.class));
        ids.add(Id.pollId(String.class));
        ids.add(Id.pollId(Y.class));
        assertEquals(6, ids.size());
    }

    private static class CloudLetA extends Cloudlet {
        public CloudLetA(int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize,
                long cloudletOutputSize, UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam,
                UtilizationModel utilizationModelBw) {
            super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu,
                    utilizationModelRam, utilizationModelBw);
        }
    }

    private static class CloudLetB extends CloudLetA {
        public CloudLetB(int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize,
                long cloudletOutputSize, UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam,
                UtilizationModel utilizationModelBw) {
            super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu,
                    utilizationModelRam, utilizationModelBw);
        }
    }

    private class X {
    }

    private class Y extends X {
    }

}
