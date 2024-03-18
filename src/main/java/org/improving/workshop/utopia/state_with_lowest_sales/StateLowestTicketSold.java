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
import java.util.Iterator;
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
    public static final String TICKET_EVENT_STREAM = "tickets_with_events";
    public static final String ENHANCED_TICKET_STREAM = "enhanced_tickets";
    public static final String TEST_TOPIC = "kafka-workshop-test";
    public static final String OUTPUT_TOPIC = "kafka-workshop-state-lowest-tickets";
    public static final JsonSerde<VenueWithAddress> SERDE_VENUE_WITH_ADDRESS_JSON = new JsonSerde<>(VenueWithAddress.class);
    public static final JsonSerde<TicketWithEvent> SERDE_TICKET_WITH_EVENT_JSON = new JsonSerde<>(TicketWithEvent.class);
    public static final JsonSerde<EnhancedTicket> SERDE_ENHANCED_TICKET_JSON = new JsonSerde<>(EnhancedTicket.class);
    public static final JsonSerde<EventStateSold> SERDE_EVENT_STATE_SOLD_JSON = new JsonSerde<>(EventStateSold.class);
    public static final JsonSerde<StateCounterMap> STATE_COUNTER_MAP_JSON_SERDE = new JsonSerde<>(StateCounterMap.class);
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

        // <editor-fold desc="Create Global Table of Venues with Addresses">
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

        KStream<String, TicketWithEvent> ticket_with_event_stream =
                builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                        .selectKey((ticket_id, ticket) -> ticket.eventid())
                        .join(eventsTable, (event_id, ticket, event) -> new TicketWithEvent(ticket.id(),
                                ticket.customerid(),
                                ticket.eventid(),
                                event.venueid(),
                                event.capacity(),
                                event.artistid(),
                                ticket.price()))
                        .peek((key,value) -> log.info("[TicketEvent] Ticket: {} for Event: {} at Venue: {}", value.id(), value.eventid(), value.venueid()))
                        .selectKey((event_id, ticket) -> ticket.venueid());
        // </editor-fold>

        // <editor-fold desc="Create Enhanced Ticket Stream">
        KStream<String, EnhancedTicket> enhanced_ticket_stream =
                ticket_with_event_stream.join(
                        global_venue_address_table,
                        (key, ticket_event) -> key,
                        (key, ticket_event, venue_with_address) -> new EnhancedTicket(ticket_event.id(),
                                                                                      ticket_event.customerid(),
                                                                                      ticket_event.eventid(),
                                                                                      ticket_event.venueid(),
                                                                                      venue_with_address.name(),
                                                                                      venue_with_address.line1(),
                                                                                      venue_with_address.line2(),
                                                                                      venue_with_address.citynm(),
                                                                                      venue_with_address.state(),
                                                                                      venue_with_address.zip5(),
                                                                                      venue_with_address.zip4(),
                                                                                      venue_with_address.countrycd(),
                                                                                      ticket_event.capacity(),
                                                                                      venue_with_address.maxcapacity(),
                                                                                      ticket_event.artistid(),
                                                                                      ticket_event.price()
                                                                                    )
                        )
                        .peek((key, ticket) -> log.info("[Enhanced Ticket] Id: {} Artist: {} Venue: {} Venue State: {}", ticket.id(), ticket.artist_id(), ticket.venueid(), ticket.venue_state()));

//        ticket_with_venue_stream.join(global_venue_address_table, (venue_id, ticket_with_venue) -> venue_id, EnhancedTicket::CreateEnhancedTicket)
//                                .peek((key, ticket) -> log.info("[Enhanced Ticket] Id: {} Artist: {} Venue: {} Venue State: {}", ticket.id(), ticket.artist_id(), ticket.venueid(), ticket.venue_state()));
        // </editor-fold>

        // <editor-fold desc="Calculate Percent Sold By Event for Each State">
        enhanced_ticket_stream
                .selectKey((venue_id, ticket) -> ticket.artist_id())
                .groupByKey(Grouped.with(null, SERDE_ENHANCED_TICKET_JSON))
                .aggregate(
                           StateCounterMap::new,
                           (event_id, enhanced_ticket, state_counter_map) ->
                              {
                                  state_counter_map.track_sold_ticket(enhanced_ticket);
                                  return state_counter_map;
                              },
                           Materialized
                              .<String, StateCounterMap>as(persistentKeyValueStore("enhanced-ticket-state-counts"))
                              .withKeySerde(Serdes.String())
                              .withValueSerde(STATE_COUNTER_MAP_JSON_SERDE)
                              )
                .toStream()
                .mapValues(sortedCounterMap -> sortedCounterMap.states_with_worst_sales(NUMBER_OF_STATES))
                .peek((event_id, state_counter_map) -> log.info("The bottom {} state(s) with the fewest sold ticket for event {} are: {}", NUMBER_OF_STATES, event_id, state_counter_map))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), PERCENT_HASH_MAP_JSON_SERDE));
        // </editor-fold>

    }

    public class EventTicketCounter {
        private final Integer event_capacity;
        private Long tickets;

        public EventTicketCounter(Integer event_capacity) {
            this.event_capacity = event_capacity;
            this.tickets = (long)0;
        }

        public void increment_ticket() {
            tickets++;
        }

        public Double percent_sold() {
            return (double)this.tickets / (double)this.event_capacity;
        }
    }

    // ASSUMPTION: all events for an artist are related to the same "tour"
    @Data
    @AllArgsConstructor
//    public static class StateCounterMap {
    public class StateCounterMap
    {
        private int event_capacity;
        private LinkedHashMap<String, LinkedHashMap<String, EventTicketCounter>> map; // Key = state;  Value = map of counts of tickets indexed by event

        public StateCounterMap() {
            this.map = new LinkedHashMap<>();
        }

        public void track_sold_ticket(EnhancedTicket enhanced_ticket)
        {
            String state = enhanced_ticket.venue_state();
            LinkedHashMap<String, EventTicketCounter> event_ticket_map = map.get(state);

            // Check if this is the first ticket for a state
            if (event_ticket_map == null)
            {
                event_ticket_map = new LinkedHashMap<>();
            }

            // Check if this is the first ticket for an event in a state
            String event_id = enhanced_ticket.eventid();
            EventTicketCounter event_ticket_counter = event_ticket_map.get(event_id);
            if (event_ticket_counter == null)
            {
                event_ticket_counter = new EventTicketCounter(enhanced_ticket.event_capacity());
            }
            event_ticket_counter.increment_ticket();
        }

        public HashMap<String, Double> states_with_worst_sales(int limit)
        {
            // Find the percent sold in each state
            LinkedHashMap<String, Double> state_percent_sold_map = new LinkedHashMap<>();
            for (Map.Entry<String, LinkedHashMap<String, EventTicketCounter>> state_entry : map.entrySet())
            {
                // For every state, find the percent sold in each event
                Double percent_sold_sum = 0.0;
                for (Map.Entry<String, EventTicketCounter> event_entry : state_entry.getValue().entrySet())
                {
                    percent_sold_sum += event_entry.getValue().percent_sold();
                }
                state_percent_sold_map.put(state_entry.getKey(), (percent_sold_sum / state_entry.getValue().size()));
            }
            return state_percent_sold_map.entrySet()
                                         .stream()
                                         .sorted(Map.Entry.comparingByValue())
                                         .limit(NUMBER_OF_STATES)
                                         .collect(LinkedHashMap::new, (map1, entry) -> map1.put(entry.getKey(), entry.getValue()), Map::putAll);
        }
    }
}