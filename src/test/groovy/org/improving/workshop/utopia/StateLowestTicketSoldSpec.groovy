package org.improving.workshop.utopia.state_with_lowest_sales

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.stream.Stream
import org.msse.demo.mockdata.music.ticket.Ticket
import org.msse.demo.mockdata.music.venue.Venue
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.EVENTS
import static org.improving.workshop.utils.DataFaker.TICKETS
import static org.improving.workshop.utils.DataFaker.EVENTS_FIXED_CAPACITY

class StateLowestTicketSoldSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Address> addressInputTopic
    TestInputTopic<String, Venue> venueInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic
    TestInputTopic<String, Event> eventInputTopic

    // outputs
    TestOutputTopic<String, HashMap<String, Double>> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the topology (by reference)
        StateLowestTicketSold.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        // instantiate input topics
        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        )

        venueInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_VENUES,
                Serdes.String().serializer(),
                Streams.SERDE_VENUE_JSON.serializer()
        )

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        )

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        )

        // instantiate output topics
        outputTopic = driver.createOutputTopic(
                StateLowestTicketSold.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                StateLowestTicketSold.PERCENT_HASH_MAP_JSON_SERDE.deserializer()
        )
    }

    def 'cleanup'() {
        driver.close()
    }

    def "States that sold the fewest tickets per event"() {
        given: 'piping the addresses through the stream'
        def mn_address_1 = new Address("mn-address-1",null,"us-numbered-street","us","123 Fake Street","","Minneapolis","MN", "55455", "1234", "USA", 44.56467, 93.13314)
        def wi_address_1 = new Address("wi-address-1",null,"us-alpha-prefix","us","W123 Fake Ave","","Dodgeville","WI", "53533", "1234", "USA", 42.57332, 90.07338)
        def ia_address_1 = new Address("ia-address-1",null,"us-numbered-street","us","321 Made Up","","Cedar Rapids","IA", "52498", "1234", "USA", 41.57381, 91.39548)
        def cust_address_1 = new Address("cust-address-1","1","us-numbered-street","us","0 Space Ball City","","New Emperor","SB", "12345", "1234", "SB", 0.0, 0.0)
        addressInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(mn_address_1.id(), mn_address_1),
                new KeyValue<String, Stream>(wi_address_1.id(), wi_address_1),
                new KeyValue<String, Stream>(ia_address_1.id(), ia_address_1),
                new KeyValue<String, Stream>(cust_address_1.id(), cust_address_1),
        ])

        and: 'piping the venues through the stream'
        def venue_1 = new Venue("mn-venue-1", "mn-address-1", "Mega-Tour-Stop-1", 500)
        def venue_2 = new Venue("wi-venue-1", "wi-address-1", "Mega-Tour-Stop-2", 500)
        def venue_3 = new Venue("ia-venue-1", "ia-address-1", "Mega-Tour-Stop-3", 500)
        venueInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(venue_1.id(), venue_1),
                new KeyValue<String, Stream>(venue_2.id(), venue_2),
                new KeyValue<String, Stream>(venue_3.id(), venue_3),
        ])

        and: 'piping events through the stream'
        def event1_artist1 = EVENTS_FIXED_CAPACITY.generate("event-1", "artist-1", "mn-venue-1", 10)
        def event2_artist1 = EVENTS_FIXED_CAPACITY.generate("event-2", "artist-1", "mn-venue-1", 10)
        def event3_artist1 = EVENTS_FIXED_CAPACITY.generate("event-3", "artist-1", "wi-venue-1", 10)
        def event4_artist2 = EVENTS_FIXED_CAPACITY.generate("event-4", "artist-1", "ia-venue-1", 10)
        eventInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(event1_artist1.id(), event1_artist1),
                new KeyValue<String, Stream>(event2_artist1.id(), event2_artist1),
                new KeyValue<String, Stream>(event3_artist1.id(), event3_artist1),
                new KeyValue<String, Stream>(event4_artist2.id(), event4_artist2),
        ])

        and: 'piping tickets through the stream'
        def customer_1_ticket_1 = TICKETS.generate("customer-1", "event-1")
        def customer_2_ticket_1 = TICKETS.generate("customer-2", "event-1")
        def customer_3_ticket_1 = TICKETS.generate("customer-3", "event-1")
        def customer_3_ticket_2 = TICKETS.generate("customer-3", "event-1")
        def customer_3_ticket_3 = TICKETS.generate("customer-3", "event-1")
        def customer_4_ticket_1 = TICKETS.generate("customer-4", "event-3")
        def customer_5_ticket_1 = TICKETS.generate("customer-5", "event-3")
        def customer_5_ticket_2 = TICKETS.generate("customer-5", "event-3")
        def customer_6_ticket_1 = TICKETS.generate("customer-6", "event-2")
        def customer_7_ticket_1 = TICKETS.generate("customer-7", "event-2")
        def customer_8_ticket_1 = TICKETS.generate("customer-8", "event-4")
        def customer_9_ticket_1 = TICKETS.generate("customer-9", "event-4")
        ticketInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(customer_1_ticket_1.id(), customer_1_ticket_1),
                new KeyValue<String, Stream>(customer_2_ticket_1.id(), customer_2_ticket_1),
                new KeyValue<String, Stream>(customer_3_ticket_1.id(), customer_3_ticket_1),
                new KeyValue<String, Stream>(customer_3_ticket_2.id(), customer_3_ticket_2),
                new KeyValue<String, Stream>(customer_3_ticket_3.id(), customer_3_ticket_3),
                new KeyValue<String, Stream>(customer_4_ticket_1.id(), customer_4_ticket_1),
                new KeyValue<String, Stream>(customer_5_ticket_1.id(), customer_5_ticket_1),
                new KeyValue<String, Stream>(customer_5_ticket_2.id(), customer_5_ticket_2),
                new KeyValue<String, Stream>(customer_6_ticket_1.id(), customer_6_ticket_1),
                new KeyValue<String, Stream>(customer_7_ticket_1.id(), customer_7_ticket_1),
                new KeyValue<String, Stream>(customer_8_ticket_1.id(), customer_8_ticket_1),
                new KeyValue<String, Stream>(customer_9_ticket_1.id(), customer_9_ticket_1),
        ])

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'records received'
        outputRecords.size() == 12

        and: 'Bottom 2 states with the fewest sales'
        def bottom_2 = outputRecords.last()

        // record is for customer 1
        bottom_2.key() == "artist-1"
        // artist 3 is back into the top 3!
        bottom_2.value() == [
                "IA": (double)0.2,
                "WI": (double)0.3
        ]

    }
}
