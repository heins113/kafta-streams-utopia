package org.improving.workshop.utopia;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.exercises.stateful.ArtistTicketCount;
import org.improving.workshop.samples.TopCustomerArtists;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;


@Slf4j
public class MostProfitableCustomers {

    public static final String OUTPUT_TOPIC = "kafka-workshop-most-profitable-customer";
    public static final JsonSerde<CustomerStreamTicketCount> CSTC_JSON_SERDE = new JsonSerde<>(CustomerStreamTicketCount.class);
    public static final JsonSerde<CustomerProfitabilityMap> CUSTOMER_PROFITABILITY_JSON_SERDE = new JsonSerde<>(CustomerProfitabilityMap.class);
    public static final JsonSerde<LinkedHashMap<String, CustomerWithProfitability>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        KTable<String, Long> customer_ticket_count_table =
                builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), Streams.SERDE_STREAM_JSON))
                       .peek((ticket_id, ticket) -> log.info("Ticket Received: {}", ticket))
                       .selectKey((ticket_id, ticket) -> ticket.customerid(), Named.as("ticket-rekey-by-customerid"))
                       .groupByKey()
                       .count();
        customer_ticket_count_table.toStream().peek((key, value) -> log.info("[COUNT] Customer '{}' has purchased a ticket {} times.", key, value));

        KTable<String, Long> customer_stream_count_table =
                builder.stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), Streams.SERDE_STREAM_JSON))
                        .peek((stream_id, stream) -> log.info("Stream Received: {}", stream))
                        .selectKey((stream_id, stream) -> stream.customerid(), Named.as("stream-rekey-by-customerid"))
                        .groupByKey()
                        .count();
        customer_stream_count_table.toStream().peek((key, value) -> log.info("[COUNT] Customer '{}' has listened to streams {} times.", key, value));

        GlobalKTable<String, Customer> global_customer_table =
                builder.globalTable(TOPIC_DATA_DEMO_CUSTOMERS,
                                    Materialized
                                        .<String, Customer>as(persistentKeyValueStore("customers"))
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(SERDE_CUSTOMER_JSON));
        // [KAH]: TBD: How do I Peek from a global table?

        customer_ticket_count_table.join(customer_stream_count_table, StreamTicketCount::new)
                .toStream()
                .join(global_customer_table,
                     (stream_ticket_count_key, stream_ticket_count_value) -> stream_ticket_count_key,
                     (stream_ticket_count, customer) -> new CustomerStreamTicketCount(customer, stream_ticket_count))
                .groupByKey(Grouped.with(null, CSTC_JSON_SERDE))
                .aggregate(
                        CustomerProfitabilityMap::new,
                        (customer_id, customer_with_counts, profitability_map) -> {
//                            if (!profitability_calculator.initialized) {
//                                profitability_calculator.initialize(customer_with_counts.customer);
//                            }
//                            profitability_calculator.calculate(customer_with_counts.counts.ticket_count, customer_with_counts.counts.stream_count);
                            profitability_map.update(customer_with_counts);
                            return profitability_map;
                        },
                        Materialized
                                .<String, CustomerProfitabilityMap>as(persistentKeyValueStore("customer-profitability-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(CUSTOMER_PROFITABILITY_JSON_SERDE)
                )
                .toStream()
                .peek((customer_id, customer_profitability) -> log.info("Customer ID: {} has a Profitability Score of: {}", customer_id, customer_profitability.profitability_score))
                .mapValues(sortedCounterMap -> sortedCounterMap.top(3))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), LINKED_HASH_MAP_JSON_SERDE));

                // ---------------- ALTERNATE (ignore if above is working) --------------------------------

//        KTable<String, Customer> customer_table =
//                builder.table(TOPIC_DATA_DEMO_CUSTOMERS,
//                              Materialized
//                                      .<String, Customer>as(persistentKeyValueStore("customers"))
//                                      .withKeySerde(Serdes.String())
//                                      .withValueSerde(SERDE_CUSTOMER_JSON));

//        customer_ticket_count_table.join(customer_stream_count_table, StreamTicketCount::new)
//                .toStream()
//                .join(customer_table, (customer_id, counts_table, customer) -> new CustomerStreamTicketCount(customer, counts_table))
//                .groupByKey(Grouped.with(null, CSTC_JSON_SERDE));

//        builder


            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
//            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StreamTicketCount {
        private Long ticket_count;
        private Long stream_count;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerStreamTicketCount {
        private Customer customer;
        private StreamTicketCount counts;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerWithProfitability {
        private Customer customer;
        private Double score;
    }

    @Data
    @AllArgsConstructor
    public static class CustomerProfitabilityMap {
//        private boolean initialized;
        private int max_size;
        private Customer customer;
        private Double profitability_score;
        private LinkedHashMap<String, CustomerWithProfitability> map;

        public CustomerProfitabilityMap() {
            this(1000);
        }

        public CustomerProfitabilityMap(int max_size) {
//            initialized = false;
            this.max_size = max_size;
            this.map = new LinkedHashMap<>();
        }

        protected double calculate(CustomerStreamTicketCount cstc) {
            double result = 0.0;
            if (cstc.counts.stream_count != 0)
            {
                result = (double)cstc.counts.ticket_count / (double)cstc.counts.stream_count;
            }
            return result;
        }

        public void update(CustomerStreamTicketCount cstc) {
            map.compute(cstc.customer.id(), (k, v) -> new CustomerWithProfitability(cstc.customer, calculate(cstc)));
//            map.compute(cstc.customer.id(), (k, v) -> v == null ? new CustomerWithProfitability(cstc.customer, profitability_score(cstc)) : profitability_score(cstc));
            // [KAH] TBD: What is the best way to update the score within v?
        }

        public LinkedHashMap<String, CustomerWithProfitability> top(int limit) {
            return map.entrySet().stream()
                    .limit(limit)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

//        public void initialize(Customer customer) {
//            this.customer = customer;
//            this.initialized = true;
//            this.profitability_score = 0.0;
//        }

//        public void calculate(long ticket_count, long stream_count) {
//            if (stream_count != 0)
//            {
//                this.profitability_score = (double)ticket_count / (double)stream_count;
//            }
//        }
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