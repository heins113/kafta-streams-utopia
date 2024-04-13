package org.improving.workshop.utopia.ticket_demographics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;
import static org.improving.workshop.Streams.*;
import static org.improving.workshop.utopia.ticket_demographics.AgedCustomer.CreateAgedCustomer;

@Slf4j
public class AgedCustomerStream {
    public static final String OUTPUT_TOPIC = "utopia-demo-aged-customer";
    public static final JsonSerde<AgedCustomer> SERDE_AGED_CUSTOMER_JSON = new JsonSerde<>(AgedCustomer.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        builder.stream(TOPIC_DATA_DEMO_CUSTOMERS, Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON))
                .peek((customer_id, customer) -> log.info("Customer Received: {}", customer))
                .map((key,value) -> KeyValue.pair(key, CreateAgedCustomer(value)))
                .peek((customer_id, customer) -> log.info("Aged Customer Received: {}", customer))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_AGED_CUSTOMER_JSON));
    }
}