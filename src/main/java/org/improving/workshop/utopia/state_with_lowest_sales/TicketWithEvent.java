package org.improving.workshop.utopia.state_with_lowest_sales;

import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import java.io.Serializable;

public record TicketWithEvent(String id, String customerid, String eventid, String venueid, Integer capacity, String artistid, Double price) implements Serializable {
    public TicketWithEvent(String id, String customerid, String eventid, String venueid, Integer capacity, String artistid, Double price) {
        this.id = id;
        this.customerid = customerid;
        this.eventid = eventid;
        this.venueid = venueid;
        this.capacity = capacity;
        this.artistid = artistid;
        this.price = price;
    }

    public static TicketWithEvent CreateTicketWithEvent(Ticket ticket, Event event) {
        return new TicketWithEvent(ticket.id(),
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
