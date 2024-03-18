package org.improving.workshop.utopia.state_with_lowest_sales;

import org.improving.workshop.utopia.state_with_lowest_sales.TicketWithEvent;
import org.improving.workshop.utopia.state_with_lowest_sales.VenueWithAddress;
import java.io.Serializable;

public record EnhancedTicket(String id, String customerid, String eventid, String venueid, String venue_name, String venue_line1, String venue_line2, String venue_citynm, String venue_state, String venue_zip5, String venue_zip4, String venue_countrycd, Integer event_capacity, Integer venue_capacity, String artist_id, Double price) implements Serializable {
    public EnhancedTicket(String id, String customerid, String eventid, String venueid, String venue_name, String venue_line1, String venue_line2, String venue_citynm, String venue_state, String venue_zip5, String venue_zip4, String venue_countrycd, Integer event_capacity, Integer venue_capacity, String artist_id, Double price) {
        this.id = id;
        this.customerid = customerid;
        this.eventid = eventid;
        this.venueid = venueid;
        this.venue_name = venue_name;
        this.venue_line1 = venue_line1;
        this.venue_line2 = venue_line2;
        this.venue_citynm = venue_citynm;
        this.venue_state = venue_state;
        this.venue_zip5 = venue_zip5;
        this.venue_zip4 = venue_zip4;
        this.venue_countrycd = venue_countrycd;
        this.event_capacity = event_capacity;
        this.venue_capacity = venue_capacity;
        this.artist_id = artist_id;
        this.price = price;
    }

    public static EnhancedTicket CreateEnhancedTicket(String key, TicketWithEvent ticket, VenueWithAddress venue) {
        return new EnhancedTicket(ticket.id(),
                                   ticket.customerid(),
                                   ticket.eventid(),
                                   ticket.venueid(),
                                   venue.name(),
                                   venue.line1(),
                                   venue.line2(),
                                   venue.citynm(),
                                   venue.state(),
                                   venue.zip5(),
                                   venue.zip4(),
                                   venue.countrycd(),
                                   ticket.capacity(),
                                   venue.maxcapacity(),
                                   ticket.artistid(),
                                   ticket.price());
    }

    public String id() {
        return this.id;
    }

    public String customerid() {
        return this.customerid;
    }

    public String eventid() {
        return this.eventid;
    }

    public String venueid() {
        return this.venueid;
    }

    public String venue_name() {
        return this.venue_name;
    }

    public String venue_line2() {
        return this.venue_line2;
    }

    public String venue_citynm() {
        return this.venue_citynm;
    }

    public String venue_state() {
        return this.venue_state;
    }

    public String venue_zip5() {
        return this.venue_zip5;
    }

    public String venue_zip4() {
        return this.venue_zip4;
    }

    public String venue_countrycd() {
        return this.venue_countrycd;
    }

    public Integer event_capacity() {
        return this.event_capacity;
    }

    public Integer venue_capacity() {
        return this.venue_capacity;
    }

    public String artist_id() {
        return this.artist_id;
    }

    public Double price() {
        return this.price;
    }
}
