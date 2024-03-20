package org.improving.workshop.service;

import net.datafaker.Faker;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.event.EventFaker;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class EventFixedCapacityFaker extends EventFaker {
    public EventFixedCapacityFaker(Faker faker) {
        super(faker);
    }

    public Event generate(String eventId, String artistId, String venueId, int venueCapacity) {
        return new Event(
                eventId,
                artistId,
                venueId,
                venueCapacity,
                faker.date().future(250, TimeUnit.DAYS).toString()
        );
    }
}
