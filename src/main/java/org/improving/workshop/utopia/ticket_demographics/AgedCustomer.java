package org.improving.workshop.utopia.ticket_demographics;

import org.msse.demo.mockdata.customer.profile.Customer;
import java.io.Serializable;
import java.time.*;

public record AgedCustomer(String id, String type, String gender, String fname, String mname, String lname, String fullname, String suffix, String title, String birthdt, String joindt, int age) implements Serializable {
    public AgedCustomer(String id, String type, String gender, String fname, String mname, String lname, String fullname, String suffix, String title, String birthdt, String joindt, int age) {
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
    }

    public static AgedCustomer CreateAgedCustomer(Customer customer) {
        int age = -1;
        try {
            Year current_year = Year.now(Clock.systemUTC());
            String birth_year = customer.birthdt().split("-")[0];
            int birth_year_value = Integer.parseInt(birth_year);
            if (birth_year.length() == 4)
            {
                age = current_year.getValue() - birth_year_value;
            }
        }
        catch (Exception ignored) {
        }

        return new AgedCustomer(customer.id(),
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
                age);
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
