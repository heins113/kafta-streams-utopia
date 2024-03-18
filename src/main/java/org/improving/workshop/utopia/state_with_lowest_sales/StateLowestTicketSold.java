package org.improving.workshop.utopia.state_with_lowest_sales;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.TopCustomerArtists;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class StateLowestTicketSold {

    private static final int NUMBER_OF_STATES = 2;
    public static final String ONLY_VENUE_ADDRESS_STREAM = "only_venue_address";
    public static final String VENUES_WITH_ADDRESS_STREAM = "venues_with_address";
    public static final String OUTPUT_TOPIC = "kafka-workshop-state-lowest-tickets";
    public static final JsonSerde<VenueWithAddress> SERDE_VENUE_WITH_ADDRESS_JSON = new JsonSerde<>(VenueWithAddress.class);
    public static final JsonSerde<TicketWithVenue> SERDE_TICKET_WITH_VENUE_JSON = new JsonSerde<>(TicketWithVenue.class);
    public static final JsonSerde<EnhancedTicket> SERDE_ENHANCED_TICKET_JSON = new JsonSerde<>(EnhancedTicket.class);
    public static final JsonSerde<EventStateSold> SERDE_EVENT_STATE_SOLD_JSON = new JsonSerde<>(EventStateSold.class);
    public static final JsonSerde<SortedStateCounterMap> STATE_COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedStateCounterMap.class);
    public static final JsonSerde<HashMap<String, Double>> PERCENT_HASH_MAP_JSON_SERDE = new JsonSerde<>(HashMap.class);

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
               .peek((key,value) -> log.info("[Venue] with an ID of: {} merged with it's address {}.", key, value.id()))
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
        eventsTable.toStream().peek((key, event) -> log.info("[Event] '{}' registered for artist '{}' at venue '{}'.", key, event.artistid(), event.venueid()));

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
                .peek((key,value) -> log.info("[TicketVenue] Ticket: {} for Event: {} at Venue: {}", value.id(), value.eventid(), value.venueid()))
                .selectKey((event_id, ticket) -> ticket.venueid());
        // </editor-fold>

        // <editor-fold desc="Create Enhanced Ticket Stream">
        KStream<String, EnhancedTicket> enhanced_ticket_stream =
        ticket_with_venue_stream.join(global_venue_address_table, (venue_id, ticket_with_venue) -> venue_id, EnhancedTicket::CreateEnhancedTicket)
                                .peek((key, ticket) -> log.info("[Enhanced Ticket] Id: {} Artist: {} Venue: {} Venue State: {}", ticket.id(), ticket.artist_id(), ticket.venueid(), ticket.venue_state()));
        // </editor-fold>

        // <editor-fold desc="Calculate Percent Sold By Event for Each State">
        enhanced_ticket_stream
                .selectKey((venue_id, ticket) -> ticket.artist_id())
                .groupByKey(Grouped.with(null, SERDE_ENHANCED_TICKET_JSON))
                .aggregate(
                           SortedStateCounterMap::new,
                           (event_id, enhanced_ticket, state_counter_map) ->
                              {
                                  state_counter_map.event_capacity = enhanced_ticket.capacity();
                                  state_counter_map.ticket_sold_in(enhanced_ticket.venue_state());
                                  return state_counter_map;
                              },
                           Materialized
                              .<String, SortedStateCounterMap>as(persistentKeyValueStore("enhanced-ticket-state-counts"))
                              .withKeySerde(Serdes.String())
                              .withValueSerde(STATE_COUNTER_MAP_JSON_SERDE)
                              )
                .toStream()
                .mapValues(sortedCounterMap -> sortedCounterMap.top(NUMBER_OF_STATES))
                .peek((event_id, state_counter_map) -> log.info("The bottom {} state(s) with the fewest sold ticket for event {} are: {}", NUMBER_OF_STATES, event_id, state_counter_map))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), PERCENT_HASH_MAP_JSON_SERDE));
        // </editor-fold>

    }

    // aggregate per event
    //      - store all 50 states
    //      - increment on each call
    // on calling "top" it calculates the % and returns the top

    @Data
    @AllArgsConstructor
    public static class SortedStateCounterMap {
        private int event_capacity;
        private LinkedHashMap<String, Long> map; // Key = state;  Value = ticket_count

//                      EventId               StateId, Count
//        private HashMap<String, LinkedHashMap<String, Long>> event_map; // HashMap of Events whose value is a SortedHashMap whose values are counts by state

//        public SortedStateCounterMap() {
//            this(1000);
//        }

        public SortedStateCounterMap() {
//            this.event_capacity = capacity;
            this.map = new LinkedHashMap<>();
        }

        public void ticket_sold_in(String venue_state) {
            map.compute(venue_state, (k, v) -> v == null ? 1 : v + 1);

            // replace with sorted map
            this.map = map.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())       // lowest first
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        /**
         * Return the top {limit} items from the counter map
         * @param limit the number of records to include in the returned map
         * @return a new LinkedHashMap with only the top {limit} elements
         */
        public HashMap<String, Double> top(int limit) {
            LinkedHashMap<String, Long> lowest = map.entrySet()
                                                    .stream()
                                                    .limit(limit)
                                                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            HashMap<String,Double> lowest_as_percent = new HashMap<>();
            for (Map.Entry<String, Long> entry : lowest.entrySet())
            {
                lowest_as_percent.put(entry.getKey(), (double)entry.getValue() / (double)this.event_capacity);
            }
            return lowest_as_percent;
//            return map.entrySet().stream()
//                    .limit(limit)
//                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }
}