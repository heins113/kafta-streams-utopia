package org.improving.workshop.utopia

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.stream.Stream
import spock.lang.Specification


import static org.improving.workshop.utils.DataFaker.STREAMS
import org.msse.demo.mockdata.music.ticket.Ticket

import static org.improving.workshop.utils.DataFaker.TICKETS

class MostProfitableCustomersSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Ticket> ticketInputTopic
    TestInputTopic<String, Stream> streamInputTopic
    TestInputTopic<String, Customer> customerInputTopic

    // outputs
    TestOutputTopic<String, String> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the MostProfitableCustomers topology (by reference)
        MostProfitableCustomers.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        // instantiate topics
        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        )

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                Serdes.String().serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        )

        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                MostProfitableCustomers.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                MostProfitableCustomers.CUSTOMER_PROFITABILITY_SERDE.deserializer()
        )

    }

    def 'cleanup'() {
        driver.close()
    }

    def "most profitable customers"() {
        given: 'piping the customers through the stream'
        def cust1 = new Customer("customer-1", "PREMIUM", "M", "John", "Steven", "James", "JSJ", "", "", "1989-01-20", "2022-01-02")
        def cust2 = new Customer("customer-2", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02")

        customerInputTopic.pipeKeyValueList([
                new KeyValue<String, Customer>(cust1.id(), cust1),
                new KeyValue<String, Customer>(cust2.id(), cust2),
        ])

        and: 'a stream for a given customer'
        def customer1_stream1 = STREAMS.generate("1", "customer-1", "artist-1")
        def customer1_stream2 = STREAMS.generate("2", "customer-1", "artist-1")
        def customer2_stream1 = STREAMS.generate("3", "customer-2", "artist-2")
        def customer2_stream2 = STREAMS.generate("4", "customer-2", "artist-3")
        def customer2_stream3 = STREAMS.generate("5", "customer-2", "artist-4")

        streamInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(customer1_stream1.id(), customer1_stream1),
                new KeyValue<String, Stream>(customer1_stream2.id(), customer1_stream2),
                new KeyValue<String, Stream>(customer2_stream1.id(), customer2_stream1),
                new KeyValue<String, Stream>(customer2_stream2.id(), customer2_stream2),
                new KeyValue<String, Stream>(customer2_stream3.id(), customer2_stream3),
        ])

        and: 'a purchased ticket for the event'
        def customer1_ticket1 = TICKETS.generate("customer-1", "event-1")
        def customer2_ticket2 = TICKETS.generate("customer-2", "event-1")

        ticketInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(customer1_ticket1.id(), customer1_ticket1),
                new KeyValue<String, Stream>(customer2_ticket2.id(), customer2_ticket2),
        ])

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: '2 record was received'
        outputRecords.size() == 2

        and: 'customer-1 has info and a profitability score of 0.5'
        outputRecords.get(0).value().customer.fname == "John"
        outputRecords.get(0).value().score == 0.5

        and: 'customer-2 has info and a profitability score of 0.33'
        outputRecords.get(1).value().customer.fname == "Jane"
        outputRecords.get(1).value().score.round(2) == (float) 0.33
    }
}
