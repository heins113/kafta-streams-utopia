package org.improving.workshop.utopia.state_with_lowest_sales;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.venue.Venue;

import java.io.Serializable;


public record VenueWithAddress(String id, String name, Integer maxcapacity, String formatcode, String type, String line1, String line2, String citynm, String state, String zip5, String zip4, String countrycd, Double latitude, Double longitude) implements Serializable {
    public VenueWithAddress(String id, String name, Integer maxcapacity, String formatcode, String type, String line1, String line2, String citynm, String state, String zip5, String zip4, String countrycd, Double latitude, Double longitude) {
        this.id = id;
        this.name = name;
        this.maxcapacity = maxcapacity;
        this.formatcode = formatcode;
        this.type = type;
        this.line1 = line1;
        this.line2 = line2;
        this.citynm = citynm;
        this.state = state;
        this.zip5 = zip5;
        this.zip4 = zip4;
        this.countrycd = countrycd;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public static VenueWithAddress CreateWithVenueAddress(Venue venue, Address address) {
        return new VenueWithAddress(venue.id(),
                                    venue.name(),
                                    venue.maxcapacity(),
                                    address.formatcode(),
                                    address.type(),
                                    address.line1(),
                                    address.line2(),
                                    address.citynm(),
                                    address.state(),
                                    address.zip5(),
                                    address.zip4(),
                                    address.countrycd(),
                                    address.latitude(),
                                    address.longitude());
    }

    public String id() {
        return this.id;
    }

    public String name() {
        return this.name;
    }

    public Integer maxcapacity() {
        return this.maxcapacity;
    }

    public String formatcode() {
        return this.formatcode;
    }

    public String type() {
        return this.type;
    }

    public String line1() {
        return this.line1;
    }

    public String line2() {
        return this.line2;
    }

    public String citynm() {
        return this.citynm;
    }

    public String state() {
        return this.state;
    }

    public String zip5() {
        return this.zip5;
    }

    public String zip4() {
        return this.zip4;
    }

    public String countrycd() {
        return this.countrycd;
    }

    public Double latitude() {
        return this.latitude;
    }

    public Double longitude() {
        return this.longitude;
    }
}
