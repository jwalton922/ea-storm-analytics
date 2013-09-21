/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mrcy.flightinformationproducer.datascraper;

import be.datablend.blueprints.impls.mongodb.MongoDBGraph;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jwalton
 */
public class FlightScheduleDataIngester {

    private ModesDataScraper modesDataScraper = new ModesDataScraper();
    private FlightScheduleScraper flightScheduleScraper = new FlightScheduleScraper();
    private Graph graph = new MongoDBGraph("localhost", 27017);

    public void ingestAirports() {
        AirportDataReader airportDataReader = new AirportDataReader();
        Collection<AirportInfo> airportList = airportDataReader.getAirportInfoMap().values();
        int count = 1;
        GraphQuery query = graph.query();
        query.has("OBJECT_TYPE", "AIRPORT");
        Iterable<Vertex> vertices = query.vertices();
        Set<String> airportNames= new HashSet<String>();
        for(Vertex v : vertices){
            airportNames.add(v.getProperty("NAME").toString());
        }
        closeIterable(vertices);
        System.out.println("Loaded existing airports");
        for (AirportInfo airport : airportList) {
            String airportName = airport.getName();

            boolean foundAirport = airportNames.contains(airportName);            
            
            if (!foundAirport) {
                System.out.println("New airport: " + airportName);
                Vertex v = graph.addVertex(null);
                v.setProperty("OBJECT_TYPE", "AIRPORT");
                v.setProperty("NAME", airportName);
                v.setProperty("LATITUDE", airport.getLat());
                v.setProperty("LONGITUDE", airport.getLon());
                v.setProperty("AIRPORT_DATA", airport.toMap());
            }
            System.out.println("Processed " + airportName + ". " + count + "/" + airportList.size());
            count++;
        }
    }

    private void closeIterable(Iterable it) {
        if (it instanceof CloseableIterable) {
            CloseableIterable closeable = (CloseableIterable) it;
            closeable.close();
        }
    }

    public void ingestData() {
        double minLat = -36;
        double minLon = -32;

        double maxLat = 71;
        double maxLon = 170;
        try {
            List<ModesData> modesData = modesDataScraper.getModesData(minLon, minLon, maxLat, maxLon);

            for (int i = 0; i < modesData.size(); i++) {

                try {
                    String flightNumber = modesData.get(i).getFlightNumber();
                    System.out.println("processing " + flightNumber + " " + i + "/" + modesData.size());
                    List<FlightScheduleData> flightScheduleData = flightScheduleScraper.getFlightSchedules(flightNumber, true);
                    if (flightScheduleData.size() == 0) {
                        System.out.println("FLIGHT: " + flightNumber + " has NO SCHEDULE DATA!!!!");
                        continue;
                    }
                    List<Map<String, Object>> fsDataMap = new ArrayList<Map<String, Object>>();
                    for (int j = 0; j < flightScheduleData.size(); j++) {
                        fsDataMap.add(flightScheduleData.get(j).toMap());
                    }
                    GraphQuery query = graph.query();
                    query.has("OBJECT_TYPE", "FLIGHT_SCHEDULE");
                    query.has("FLIGHT_NUMBER", flightNumber);

                    Iterable<Vertex> vertices = query.vertices();



                    int count = 0;
                    for (Vertex v : vertices) {
                        v.setProperty("FLIGHT_SCHEDULE_DATA", fsDataMap);
                        count++;
                    }

                    if (count <= 0) {
                        Vertex v = graph.addVertex(null);
                        v.setProperty("OBJECT_TYPE", "FLIGHT_SCHEDULE");
                        v.setProperty("FLIGHT_NUMBER", flightNumber);
                        v.setProperty("FLIGHT_SCHEDULE_DATA", fsDataMap);
                    }

                    System.out.println("finished processing " + flightNumber);

                } catch (Exception e) {
                    System.out.println("Exception processing aircraft " + e.getLocalizedMessage());
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        FlightScheduleDataIngester ingester = new FlightScheduleDataIngester();

//        ingester.ingestData();
        ingester.ingestAirports();
    }
}
