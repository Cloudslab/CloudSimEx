package org.cloudbus.cloudsim.ex.geolocation.geoip2;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GeoIP2IPGeneratorTest {

    private static File TEST_FILE = new File("/test-resources/GeoIPCountryTest.csv");
    
    private GeoIP2IPGenerator generator;
    
    @Before
    public void setUp() throws Exception {
//	generator = new GeoIP2IPGenerator(countryCodes, TEST_FILE);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
	fail("Not yet implemented");
    }

}
