/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author jwalton
 */
public class Position implements Serializable{
    
    private double lat;
    private double lon;
    private long timestamp;
    private double heading = 0.0;
    private Map<String,Object> metadata = new HashMap<String,Object>();
    
    public Position(double lat, double lon, long timestamp){
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getHeading() {
        return heading;
    }

    public void setHeading(double heading) {
        this.heading = heading;
    }
    
    public Map<String,Object> getMetadata(){
        return this.metadata;
    }
    
    public void setProperty(String key, Object value){
        this.metadata.put(key, value);
    }
    
    public Object getPropetyValue(String key){
        return this.metadata.get(key);
    }
    
    @Override
    public String toString(){
        Date date = new Date();
        date.setTime(timestamp);
        return this.lat+", "+this.lon+" "+date.toString();
    }
}
