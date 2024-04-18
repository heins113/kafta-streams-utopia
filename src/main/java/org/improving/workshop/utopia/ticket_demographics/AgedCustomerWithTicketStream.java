package org.improving.workshop.utopia.ticket_demographics;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;
import static org.improving.workshop.Streams.SERDE_TICKET_JSON;
import static org.improving.workshop.utopia.ticket_demographics.AgedCustomer.CreateAgedCustomer;
import static org.improving.workshop.utopia.ticket_demographics.AgedCustomerWithTicket.CreateAgedCustomerWithTicket;

@Slf4j
public class AgedCustomerWithTicketStream {
    public static final String OUTPUT_TOPIC = "utopia-demo-customer-ticket-genre";
    public static final JsonSerde<TicketEvent> TICKET_EVENT_JSON_SERDE = new JsonSerde<>(TicketEvent.class);
    public static final JsonSerde<ArtistTicketEvent> ARTIST_TICKET_JSON_SERDE = new JsonSerde<>(ArtistTicketEvent.class);
    public static final JsonSerde<TicketGenre> TICKET_GENRE_SERDE = new JsonSerde<>(TicketGenre.class);

    public static final JsonSerde<AgedCustomer> SERDE_AGED_CUSTOMER_JSON = new JsonSerde<>(AgedCustomer.class);

    public static final JsonSerde<AgedCustomerWithTicket> SERDE_AGED_CUSTOMER_WITH_TICKET_JSON = new JsonSerde<>(AgedCustomerWithTicket.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        KTable<String, AgedCustomer> aged_customer_table = builder.stream(TOPIC_DATA_DEMO_CUSTOMERS, Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON))
                .peek((customer_id, customer) -> log.info("Customer Received: {}", customer))
                .map((key,value) -> KeyValue.pair(key, CreateAgedCustomer(value)))
                .peek((customer_id, customer) -> log.info("Aged Customer Received: {}", customer))
                .toTable(Materialized.<String, AgedCustomer>as(persistentKeyValueStore("aged-customers"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SERDE_AGED_CUSTOMER_JSON));

        GlobalKTable<String, Artist> global_artist_table =
                builder.globalTable(TOPIC_DATA_DEMO_ARTISTS,
                        Materialized.<String, Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_ARTIST_JSON));

        KTable<String, Event> event_table =
                builder.table(TOPIC_DATA_DEMO_EVENTS,
                        Materialized.<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON));

        builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticket) -> log.info("Ticket Received: {}", ticket))
                .selectKey((ticketId, ticket) -> ticket.eventid())
                .join(
                        event_table,
                        (eventId, ticket, event) -> new TicketEvent(ticket, event))
                .selectKey((ticketId, ticketEvent) -> ticketEvent.event.artistid())
                .join(
                        global_artist_table,
                        (artistId, event) -> artistId,
                        (artistId, ticketEvent, artist) -> new ArtistTicketEvent(artist, ticketEvent.ticket, ticketEvent.event))
                .mapValues((artistId, artistTicketEvent) -> new TicketGenre(artistTicketEvent.ticket.id(),
                        artistTicketEvent.ticket.customerid(),
                        artistTicketEvent.ticket.eventid(),
                        artistTicketEvent.ticket.price(),
                        artistTicketEvent.artist.genre()))
                .selectKey((artistId, ticketGenre) -> ticketGenre.customerid)
                .join(
                        aged_customer_table,
                        (customerId, ticketGenre, agedCustomer) -> CreateAgedCustomerWithTicket(agedCustomer, ticketGenre))
                .peek((customerId, customerWithTicket) -> log.info("[OUTPUT] Ticket '{}' with genre '{}' has been created for '{}'", customerWithTicket.ticketId(), customerWithTicket.genre(), customerWithTicket.id()))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_AGED_CUSTOMER_WITH_TICKET_JSON));
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class TicketEvent {
        private Ticket ticket;
        private Event event;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class ArtistTicketEvent {
        private Artist artist;
        private Ticket ticket;
        private Event event;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class TicketGenre {
        public String id;
        public String customerid;;
        public String eventid;
        public Double price;
        public String genre;
    }
}
