package org.cloudbus.cloudsim.ex.geolocation;

import static org.junit.Assert.assertEquals;

import java.net.UnknownHostException;

import org.junit.Test;

public class IPUtilTest {

    @Test
    public void testIPv4StringConversion() throws UnknownHostException {
        assertEquals("0.0.0.1", IPUtil.convertIPv4(1));
        assertEquals("0.0.0.2", IPUtil.convertIPv4(2));
        assertEquals("127.255.255.255", IPUtil.convertIPv4(Integer.MAX_VALUE));
        assertEquals("128.0.0.0", IPUtil.convertIPv4(Integer.MIN_VALUE));
        assertEquals("255.255.255.255", IPUtil.convertIPv4(-1));
        assertEquals("223.255.255.0", IPUtil.convertIPv4((int) 3758096128l));
        assertEquals("182.48.63.255", IPUtil.convertIPv4((int) 3056615423l));
    }

    // private static byte[] conv(int value) {
    // return new byte[] {
    // (byte) (value >>> 24),
    // (byte) (value >>> 16),
    // (byte) (value >>> 8),
    // (byte) value };
    //
    // }
}
