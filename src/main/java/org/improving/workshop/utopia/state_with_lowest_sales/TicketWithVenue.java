package org.improving.workshop.utopia.state_with_lowest_sales;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.event.Event;

import java.io.Serializable;

public record TicketWithVenue(String id, String customerid, String eventid, String venueid, Integer capacity, String artistid, Double price) implements Serializable {
    public TicketWithVenue(String id, String customerid, String eventid, String venueid, Integer capacity, String artistid, Double price) {
        this.id = id;
        this.customerid = customerid;
        this.eventid = eventid;
        this.venueid = venueid;
        this.capacity = capacity;
        this.artistid = artistid;
        this.price = price;
    }

    public static TicketWithVenue CreateTicketWithVenue(Ticket ticket, Event event) {
        return new TicketWithVenue(ticket.id(),
                                   ticket.customerid(),
                                   ticket.eventid(),
                                   event.venueid(),
                                   event.capacity(),
                                   event.artistid(),
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

    public Integer capacity() {
        return this.capacity;
    }

    public String artistid() {
        return this.artistid;
    }

    public Double price() {
        return this.price;
    }
}
