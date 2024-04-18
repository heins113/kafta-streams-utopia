package org.improving.workshop.utopia.ticket_demographics;

import org.msse.demo.mockdata.customer.profile.Customer;

import java.time.Clock;
import java.time.Year;

public record AgedCustomerWithTicket(String id, String type, String gender, String fname, String mname, String lname,
                                     String fullname, String suffix, String title, String birthdt, String joindt, int age, String ageRange, String ticketId, String eventid, Double price, String genre) {
    public AgedCustomerWithTicket(String id, String type, String gender, String fname, String mname, String lname,
                                  String fullname, String suffix, String title, String birthdt, String joindt, int age, String ageRange, String ticketId, String eventid, Double price, String genre) {
        this.id = id;
        this.type = type;
        this.gender = gender;
        this.fname = fname;
        this.mname = mname;
        this.lname = lname;
        this.fullname = fullname;
        this.suffix = suffix;
        this.title = title;
        this.birthdt = birthdt;
        this.joindt = joindt;
        this.age = age;
        this.ageRange = ageRange;
        this.ticketId = ticketId;
        this.eventid = eventid;
        this.price = price;
        this.genre = genre;
    }

    public static AgedCustomerWithTicket CreateAgedCustomerWithTicket(AgedCustomer customer, AgedCustomerWithTicketStream.TicketGenre ticketGenre) {
        return new AgedCustomerWithTicket(customer.id(),
                customer.type(),
                customer.gender(),
                customer.fname(),
                customer.mname(),
                customer.lname(),
                customer.fullname(),
                customer.suffix(),
                customer.title(),
                customer.birthdt(),
                customer.joindt(),
                customer.age(),
                customer.ageRange(),
                ticketGenre.id,
                ticketGenre.eventid,
                ticketGenre.price,
                ticketGenre.genre);
    }

    public String id() {
        return this.id;
    }

    public String type() {
        return this.type;
    }

    public String gender() {
        return this.gender;
    }

    public String fname() {
        return this.fname;
    }

    public String mname() {
        return this.mname;
    }

    public String lname() {
        return this.lname;
    }

    public String fullname() {
        return this.fullname;
    }

    public String suffix() {
        return this.suffix;
    }

    public String title() {
        return this.title;
    }

    public String birthdt() {
        return this.birthdt;
    }

    public String joindt() {
        return this.joindt;
    }

    public int age() {
        return this.age;
    }
}
