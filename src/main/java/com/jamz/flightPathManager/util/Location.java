package com.jamz.flightPathManager.util;

public class Location {
    public double latitude, longitude, altitude;

    public Location(double latitude, double longitude, double altitude) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
    }

    /**
     * Gets the ground distance in metres between two LatLon objects.
     *
     * This method is an approximation, and will not be accurate over large distances and close to the
     * earth's poles.
     *
     * @param other the other point to measure distance to
     * @return the distance from this point to the other point
     */
    public double getDistanceTo(Location other) {
        double dlat = other.latitude - this.latitude;
        double dlon = other.longitude - this.longitude;

        return Math.sqrt((dlat * dlat) + (dlon * dlon))  * 1.113195e5;
    }
}
