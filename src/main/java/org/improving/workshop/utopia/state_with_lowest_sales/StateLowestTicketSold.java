package org.improving.workshop.utopia.state_with_lowest_sales;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class StateLowestTicketSold {

    public static final String ONLY_VENUE_ADDRESS_STREAM = "only_venue_address";
    public static final String VENUES_WITH_ADDRESS_STREAM = "venues_with_address";
    public static final String OUTPUT_TOPIC = "kafka-workshop-state-lowest-tickets";
    public static final JsonSerde<VenueWithAddress> SERDE_VENUE_WITH_ADDRESS_JSON = new JsonSerde<>(VenueWithAddress.class);
    public static final JsonSerde<TicketWithVenue> SERDE_TICKET_WITH_VENUE_JSON = new JsonSerde<>(TicketWithVenue.class);
    public static final JsonSerde<EnhancedTicket> SERDE_ENHANCED_TICKET_JSON = new JsonSerde<>(EnhancedTicket.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        // <editor-fold desc="Create Global Table of Addresses of Only Venues">
        builder.stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
               .filter((key,value) -> value.customerid() == null)
               .peek((key,value) -> log.info("[Address] for a venue registered with an ID of: {}", key))
               .to(ONLY_VENUE_ADDRESS_STREAM, Produced.with(Serdes.String(), SERDE_ADDRESS_JSON));
        GlobalKTable<String, Address> global_addresses_of_venues_table =
                builder.globalTable(ONLY_VENUE_ADDRESS_STREAM,
                                    Materialized
                                        .<String, Address>as(persistentKeyValueStore("global-only-venue-address-table"))
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(SERDE_ADDRESS_JSON));
        // </editor-fold>

        // <editor-fold desc="Create Global Table of Addresses of Only Venues">
        builder.stream(TOPIC_DATA_DEMO_VENUES, Consumed.with(Serdes.String(), SERDE_VENUE_JSON))
               .selectKey((key,value) -> value.addressid())
               .join(global_addresses_of_venues_table, (address_id, address) -> address_id, VenueWithAddress::CreateWithVenueAddress)
               .selectKey((key,value) -> value.id())
               .peek((key,value) -> log.info("[Venue] with an ID of: {} merged with it's address.", key))
               .to(VENUES_WITH_ADDRESS_STREAM, Produced.with(Serdes.String(), SERDE_VENUE_WITH_ADDRESS_JSON));
        GlobalKTable<String, VenueWithAddress> global_venue_address_table =
                builder.globalTable(VENUES_WITH_ADDRESS_STREAM,
                                    Materialized
                                        .<String, VenueWithAddress>as(persistentKeyValueStore("global-venue-with-address-table"))
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(SERDE_VENUE_WITH_ADDRESS_JSON));
        // </editor-fold>

        // <editor-fold desc="Create Event-Ticket association">
        KTable<String, Event> eventsTable =
                builder.table(TOPIC_DATA_DEMO_EVENTS,
                              Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_EVENT_JSON));
        eventsTable.toStream().peek((key, event) -> log.info("Event '{}' registered for artist '{}' at venue '{}'.", key, event.artistid(), event.venueid()));

        KStream<String, TicketWithVenue> ticket_with_venue_stream =
        builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .selectKey((ticket_id, ticket) -> ticket.eventid())
                .join(eventsTable, (event_id, ticket, event) -> new TicketWithVenue(ticket.id(),
                                                                                    ticket.customerid(),
                                                                                    ticket.eventid(),
                                                                                    event.venueid(),
                                                                                    event.capacity(),
                                                                                    event.artistid(),
                                                                                    ticket.price()))
                .selectKey((event_id, ticket) -> ticket.venueid());
        // </editor-fold>

        // <editor-fold desc="Create Enhanced Ticket">
        ticket_with_venue_stream.join(global_venue_address_table, (venue_id, ticket_with_venue) -> venue_id, EnhancedTicket::CreateEnhancedTicket)
                                .selectKey((venue_id, ticket) -> ticket.venue_state())
                                .groupByKey(Grouped.with(null, SERDE_ENHANCED_TICKET_JSON));
        // </editor-fold>


    }

    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;
        private LinkedHashMap<String, Long> map;

        public SortedCounterMap() {
            this(1000);
        }

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.map = new LinkedHashMap<>();
        }

        public void incrementCount(String id) {
            map.compute(id, (k, v) -> v == null ? 1 : v + 1);

            // replace with sorted map
            this.map = map.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue()))
                    // keep a limit on the map size
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        /**
         * Return the top {limit} items from the counter map
         * @param limit the number of records to include in the returned map
         * @return a new LinkedHashMap with only the top {limit} elements
         */
        public LinkedHashMap<String, Long> top(int limit) {
            return map.entrySet().stream()
                    .limit(limit)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }
}