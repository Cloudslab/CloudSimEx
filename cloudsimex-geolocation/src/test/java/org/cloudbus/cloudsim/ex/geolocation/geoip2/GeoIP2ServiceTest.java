package org.cloudbus.cloudsim.ex.geolocation.geoip2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.cloudbus.cloudsim.ex.geolocation.geoip2.GeoIP2Service;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class GeoIP2ServiceTest {

    private static final String MELBOURNE_IP = "128.250.180.0";
    private static final String SYDNEY_IP = "203.27.21.0";
    private static final String NEW_YORK_IP = "74.221.217.130";
    private static final String LONDON_IP = "192.165.213.0";
    
    private static final double DISTANCE_SYDNEY_NEW_YORK_KM = 15990;
    private static final double DISTANCE_SYDNEY_MELBOURNE_KM = 712.35;
    private static final double DISTANCE_SYDNEY_LONDON_KM = 16983.04;
    private static final double DISTANCE_LONDON_NEW_YORK_KM = 5576.74;

    private static final int DISTANCE_COMPARISON_DELTA_KM = 20;

    private static GeoIP2Service service;

    @BeforeClass
    public static void setUp() throws Exception {
	CustomLog.configLogger(TestUtil.LOG_PROPS);
	service = new GeoIP2Service(new File("GeoLite2-City.mmdb"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailedServiceCreation() throws IOException {
	try (GeoIP2Service geoService = new GeoIP2Service(new File("./nonexisting"))){
	    //pass
	}
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIPMultipleAddresses() {
	service.getCoordinates("1.2.3.4.5");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIPOutOfRange() {
	service.getCoordinates("1.256.3.4");
    }

    @Test
    public void testGetMelborneIPData() {
	String[] result = service.getMetaData(MELBOURNE_IP);
	assertEquals("Oceania", result[0]);
	assertEquals("OC", result[1]);
	assertEquals("Australia", result[2]);
	assertEquals("AU", result[3]);
	assertEquals("Melbourne", result[4]);
	assertEquals(-37.8139, Double.parseDouble(result[6]), 0.01);
	assertEquals(144.9634, Double.parseDouble(result[7]), 0.01);
	
	Double[] coords = service.getCoordinates(MELBOURNE_IP);
	assertEquals(-37.8139, coords[0], 0.01);
	assertEquals(144.9634, coords[1], 0.01);
    }

    @Test
    public void testGetNewYorkIPData() {
	String[] result = service.getMetaData(NEW_YORK_IP);
	assertEquals("North America", result[0]);
	assertEquals("NA", result[1]);
	assertEquals("United States", result[2]);
	assertEquals("US", result[3]);
	assertEquals("New York", result[4]);
	assertEquals(40.7143, Double.parseDouble(result[6]), 0.01);
	assertEquals(-74.006, Double.parseDouble(result[7]), 0.01);
	
	Double[] coords = service.getCoordinates(NEW_YORK_IP);
	assertEquals(40.7143, coords[0], 0.01);
	assertEquals(-74.006, coords[1], 0.01);
    }
    
    @Test
    public void testDistanceLondonNewYork(){
	Double[] newYork = service.getCoordinates(NEW_YORK_IP);
	Double[] london = service.getCoordinates(LONDON_IP);
	double distanceLondonToNewYorkKM = service.distance(london, newYork) / 1000;
	double distanceNewYorkToLondonKM = service.distance(newYork, london) / 1000;

	assertEquals(DISTANCE_LONDON_NEW_YORK_KM, distanceLondonToNewYorkKM, DISTANCE_COMPARISON_DELTA_KM);
	assertEquals(DISTANCE_LONDON_NEW_YORK_KM, distanceNewYorkToLondonKM, DISTANCE_COMPARISON_DELTA_KM);
    }
    
    @Test
    public void testDistanceSydneyNewYork(){
	Double[] sydney = service.getCoordinates(SYDNEY_IP);
	Double[] newYork = service.getCoordinates(NEW_YORK_IP);
	double distanceSydneyToNewYorkKM = service.distance(sydney, newYork) / 1000;
	double distanceNewYorkToSydneyKM = service.distance(newYork, sydney) / 1000;

	assertEquals(DISTANCE_SYDNEY_NEW_YORK_KM, distanceSydneyToNewYorkKM, DISTANCE_COMPARISON_DELTA_KM);
	assertEquals(DISTANCE_SYDNEY_NEW_YORK_KM, distanceNewYorkToSydneyKM, DISTANCE_COMPARISON_DELTA_KM);
    }
    
    @Test
    public void testDistanceSydneyLondon(){
	Double[] sydney = service.getCoordinates(SYDNEY_IP);
	Double[] london = service.getCoordinates(LONDON_IP);
	double distanceSydneyToLondonKM = service.distance(sydney, london) / 1000;
	double distanceLondonToSydneyKM = service.distance(london, sydney) / 1000;
	
	assertEquals(DISTANCE_SYDNEY_LONDON_KM, distanceSydneyToLondonKM, DISTANCE_COMPARISON_DELTA_KM);
	assertEquals(DISTANCE_SYDNEY_LONDON_KM, distanceLondonToSydneyKM, DISTANCE_COMPARISON_DELTA_KM);
    }

    @Test
    public void testDistanceSydneyMelbourne(){
	Double[] sydney = service.getCoordinates(SYDNEY_IP);
	Double[] melbourne = service.getCoordinates(MELBOURNE_IP);
	double distanceSydneyToMelbourneKM = service.distance(sydney, melbourne) / 1000;
	double distanceMelbourneToSydneyKM = service.distance(melbourne, sydney) / 1000;

	assertEquals(DISTANCE_SYDNEY_MELBOURNE_KM, distanceSydneyToMelbourneKM, DISTANCE_COMPARISON_DELTA_KM);
	assertEquals(DISTANCE_SYDNEY_MELBOURNE_KM, distanceMelbourneToSydneyKM, DISTANCE_COMPARISON_DELTA_KM);
    }
    
    @AfterClass
    public static void tearDown() throws IOException {
	service.close();
    }

}
