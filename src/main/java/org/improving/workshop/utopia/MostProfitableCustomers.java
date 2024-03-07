package org.improving.workshop.utopia;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;

import org.msse.demo.mockdata.customer.profile.Customer;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;


@Slf4j
public class MostProfitableCustomers {

    public static final String OUTPUT_TOPIC = "kafka-workshop-most-profitable-customer";
    public static final JsonSerde<CustomerWithProfitability> CUSTOMER_PROFITABILITY_SERDE = new JsonSerde<>(CustomerWithProfitability.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        KTable<String, Long> customer_ticket_count_table =
                builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), Streams.SERDE_TICKET_JSON))
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
                        Materialized.<String, Customer>as(persistentKeyValueStore("customers"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_CUSTOMER_JSON));


        customer_ticket_count_table.join(customer_stream_count_table, StreamTicketCount::new)
                .toStream()
                .mapValues(stream_ticket_count -> (float) stream_ticket_count.ticket_count /  stream_ticket_count.stream_count)
                .join(global_customer_table,
                     (customer_id, score) -> customer_id,
                     (customer_id, score, customer) -> new CustomerWithProfitability(customer, score))
                .peek((key, value) ->  log.info("Customer {}'s Profitability Score: {}", key, value.score))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CUSTOMER_PROFITABILITY_SERDE));
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
    public static class CustomerWithProfitability {
        private Customer customer;
        private Float score;
    }
}