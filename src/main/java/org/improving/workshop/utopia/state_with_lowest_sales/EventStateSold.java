package org.improving.workshop.utopia.state_with_lowest_sales;

import java.io.Serializable;

public record EventStateSold(String eventid, Double percentSold) implements Serializable {
    public EventStateSold(String eventid, Double percentSold) {
        this.eventid = eventid;
        this.percentSold = percentSold;
    }

    public String eventid() {
        return this.eventid;
    }

    public Double price() {
        return this.percentSold;
    }
}
