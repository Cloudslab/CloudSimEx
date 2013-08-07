package org.cloudbus.cloudsim.ex.geolocation;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.logging.Level;

import org.cloudbus.cloudsim.ex.util.CustomLog;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.City;

/**
 * 
 * Implements an offline {@link IGeolocationService} from the data downloaded
 * from <a href="http://www.maxmind.com">GeoLite2</a>.
 * 
 * @author nikolay.grozev
 * 
 */
public class GeoIP2Service extends BaseGeolocationService implements IGeolocationService, Closeable {

    private DatabaseReader reader;

    /**
     * Constructor.
     * @param f - a valid file in the mmdb format.
     */
    public GeoIP2Service(final File f) {
	super();
	try {
	    reader = new DatabaseReader(f);
	} catch (IOException e) {
	    String msg = "Invalid file: " + Objects.toString(f) + " Error details:" + e.getMessage();
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException(msg, e);
	}
    }

    @Override
    public Double[] getCoordinates(String ip) {
	City city;
	try {
	    city = reader.city(InetAddress.getByName(ip));
	    return new Double[] { city.getLocation().getLatitude(),
		    city.getLocation().getLongitude() };
	} catch (UnknownHostException e) {
	    String msg = "Invalid IP: " + Objects.toString(ip);
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException("Invalid IP", e);
	} catch (IOException | GeoIp2Exception e) {
	    String msg = "Could not locate IP: " + Objects.toString(ip) + ", because " + e.getMessage();
	    CustomLog.logError(Level.INFO, msg, e);
	    return new Double[] { null, null };
	}
    }

    @Override
    public String[] getMetaData(String ip) {
	City city;
	try {
	    city = reader.city(InetAddress.getByName(ip));
	    return new String[] { city.getContinent().getName(),
		    city.getContinent().getCode(),
		    city.getCountry().getName(),
		    city.getCountry().getIsoCode(),
		    city.getCity().getName(),
		    city.getPostal().getCode(),
		    Double.toString(city.getLocation().getLatitude()),
		    Double.toString(city.getLocation().getLongitude()) };
	} catch (UnknownHostException e) {
	    String msg = "Invalid IP: " + Objects.toString(ip);
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException("Invalid IP", e);
	} catch (IOException | GeoIp2Exception e) {
	    String msg = "Could not locate IP: " + Objects.toString(ip) + ", because: " + e.getMessage();
	    CustomLog.logError(Level.INFO, msg, e);
	    return new String[8];
	}
    }

    
    @Override
    public void close() throws IOException {
	reader.close();
    }

}
