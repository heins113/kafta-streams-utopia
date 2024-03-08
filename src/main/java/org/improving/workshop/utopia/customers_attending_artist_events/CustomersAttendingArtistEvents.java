package org.improving.workshop.utopia.customers_attending_artist_events;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class CustomersAttendingArtistEvents {
    public static final String OUTPUT_TOPIC = "kafka-workshop-customer-attending-artist-events";

    public static final JsonSerde<ArtistEvent> ARTIST_EVENT_JSON_SERDE = new JsonSerde<>(ArtistEvent.class);
    public static final JsonSerde<TicketEvent> TICKET_EVENT_JSON_SERDE = new JsonSerde<>(TicketEvent.class);
    public static final JsonSerde<CustomerArtistEventCountMap> CUSTOMER_ARTIST_EVENT_COUNT_JSON_SERDE = new JsonSerde<>(CustomerArtistEventCountMap.class);
    public static final JsonSerde<CustomerInfoArtistEventCount> CUSTOMER_INFO_ARTIST_EVENT_COUNT_JSON_SERDE = new JsonSerde<>(CustomerInfoArtistEventCount.class);
    public static final JsonSerde<CustomerInfoArtistInfoEventCount> CUSTOMER_INFO_ARTIST_INFO_EVENT_COUNT_JSON_SERDE = new JsonSerde<>(CustomerInfoArtistInfoEventCount.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        GlobalKTable<String, Customer> global_customer_table =
                builder.globalTable(TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized.<String, Customer>as(persistentKeyValueStore("customers"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_CUSTOMER_JSON));

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

        KTable<String, Long> artistEventCountTable =
                event_table.toStream()
                        .peek((eventId, event) -> log.info("Event Requested: {}", event))
                        .selectKey((k, v) -> v.artistid())
                        .join(
                                global_artist_table,
                                (artistId, event) -> artistId,
                                (artistId, event, artist) -> new ArtistEvent(artist, event)
                        )
                        .groupByKey(Grouped.with(Serdes.String(), ARTIST_EVENT_JSON_SERDE))
                        .count();
        artistEventCountTable.toStream().peek((artistId, count) -> log.info("[COUNT] Artist '{}' has had {} total events", artistId, count));

        KTable<String, CustomerArtistEventCountMap> customerArtistEventCountTable =
                builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), Streams.SERDE_TICKET_JSON))
                        .peek((ticketId, ticket) -> log.info("Ticket Requested: {}", ticket))
                        .selectKey((ticketId, ticket) -> ticket.eventid())
                        .join(
                                event_table,
                                (eventId, ticket, event) -> new TicketEvent(ticket, event))
                        .selectKey((ticketId, ticketEvent) -> ticketEvent.ticket.customerid())
                        .groupByKey(Grouped.with(null, TICKET_EVENT_JSON_SERDE))
                        .aggregate(
                                // initializer
                                CustomerArtistEventCountMap::new,

                                // aggregator
                                (customerId, ticketEvent, customerArtistEventCountMap) -> {
                                    customerArtistEventCountMap.setCustomerId(customerId);
                                    customerArtistEventCountMap.incrementEventCounter(ticketEvent.event.artistid());
                                    return customerArtistEventCountMap;
                                },

                                // ktable (materialized) configuration
                                Materialized
                                        .<String, CustomerArtistEventCountMap>as(persistentKeyValueStore("ticket-event-counts"))
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(CUSTOMER_ARTIST_EVENT_COUNT_JSON_SERDE)
                        );

        customerArtistEventCountTable.toStream()
                .flatMapValues((customerId, customerArtistEventCountMap) -> {
                    ArrayList<CustomerArtistEventCount> customerArtistArrayList = new ArrayList();

                    for (HashMap.Entry<String, Long> artistCount : customerArtistEventCountMap.eventCounterMap.entrySet()) {
                        CustomerArtistEventCount customerArtistEvent = new CustomerArtistEventCount();
                        customerArtistEvent.setCustomerId(customerId);
                        customerArtistEvent.setArtistId(artistCount.getKey());
                        customerArtistEvent.setCount(artistCount.getValue());

                        customerArtistArrayList.add(customerArtistEvent);
                    }
                    return customerArtistArrayList;
                })
                .peek((customerId, customerArtistEventCount) -> log.info("[COUNT] Customer '{}' has seen '{}' {} times ", customerId, customerArtistEventCount.artistId, customerArtistEventCount.count))
                .join(
                        global_customer_table,
                        (customerId, customer) -> customerId,
                        (customerId, customer, customerArtistEventCount) -> new CustomerInfoArtistEventCount(customerArtistEventCount, customer)
                )
                .selectKey((customerId, customerInfoArtistEventCount) -> customerInfoArtistEventCount.customerArtistEventCount.artistId)
                /* Needed to register the CustomerInfoArtistEventCount, only way to do so (for now) is to groubByKey and create
                    a trivial aggregate where the in event is the same as the out event.
                */
                .groupByKey(Grouped.with(null, CUSTOMER_INFO_ARTIST_EVENT_COUNT_JSON_SERDE))
                .aggregate(
                        // initializer
                        CustomerInfoArtistEventCount::new,

                        // aggregator
                        (customerId, eventIn, eventOut) -> {
                            eventOut.setCustomer(eventIn.getCustomer());
                            eventOut.setCustomerArtistEventCount(eventIn.getCustomerArtistEventCount());
                            return eventOut;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, CustomerInfoArtistEventCount>as(persistentKeyValueStore("ticket-event-counts-register-serde"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(CUSTOMER_INFO_ARTIST_EVENT_COUNT_JSON_SERDE)
                )
                .toStream()
                /* End registration */
                .join(
                        artistEventCountTable,
                        (artistId, customerInfoArtistEventCount, artistCount) -> new CustomerInfoArtistInfoEventCount(customerInfoArtistEventCount, artistCount))
                .filter((artistId, customerInfoArtistInfoEventCount) ->
                     ((float) customerInfoArtistInfoEventCount.customerInfoArtistEventCount.customerArtistEventCount.count / customerInfoArtistInfoEventCount.artistEventCount) >= 0.5
                )
                .peek((artistId, customerInfoArtistInfoEventCount) -> log.info("[OUTPUT] Customer '{}' has seen more than 50% of '{}' shows", customerInfoArtistInfoEventCount.customerInfoArtistEventCount.customer.id(), artistId))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CUSTOMER_INFO_ARTIST_INFO_EVENT_COUNT_JSON_SERDE));

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class ArtistEvent {
        private Artist artist;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class TicketEvent {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class CustomerArtistEventCountMap {
        private String customerId;

        public HashMap<String, Long> eventCounterMap = new HashMap<>();

        public void setCustomerId(String customerId) {
            this.customerId = customerId;
        }

        public void setEventCounterMap(HashMap<String, Long> eventCounterMap) {
            this.eventCounterMap = eventCounterMap;
        }

        public void incrementEventCounter(String artistId) {
            if (eventCounterMap.containsKey(artistId)) {
                eventCounterMap.merge(artistId, 1L, Long::sum);
            } else {
                eventCounterMap.put(artistId, 1L);
            }
        }

        public void addNewEvent(String artistId, Long count) {
            eventCounterMap.put(artistId, count);
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class CustomerArtistEventCount {
        public String customerId;

        public String artistId;

        public Long count;

        public void setCustomerId(String customerId) {
            this.customerId = customerId;
        }

        public void setArtistId(String artistId) {
            this.artistId = artistId;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class CustomerInfoArtistEventCount {
        public Customer customer;
        public CustomerArtistEventCount customerArtistEventCount;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    public static class CustomerInfoArtistInfoEventCount {
        public CustomerInfoArtistEventCount customerInfoArtistEventCount;
        public Long artistEventCount;
    }
}