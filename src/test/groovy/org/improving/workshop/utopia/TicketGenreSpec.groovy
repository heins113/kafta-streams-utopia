package org.improving.workshop.utopia

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.improving.workshop.Streams
import org.improving.workshop.utopia.ticket_demographics.TicketGenreStream
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.stream.Stream
import org.msse.demo.mockdata.music.ticket.Ticket
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.EVENTS
import static org.improving.workshop.utils.DataFaker.EVENTS
import static org.improving.workshop.utils.DataFaker.EVENTS
import static org.improving.workshop.utils.DataFaker.EVENTS
import static org.improving.workshop.utils.DataFaker.TICKETS
import static org.improving.workshop.utils.DataFaker.TICKETS
import static org.improving.workshop.utils.DataFaker.TICKETS
import static org.improving.workshop.utils.DataFaker.TICKETS
import static org.improving.workshop.utils.DataFaker.TICKETS

class TicketGenreSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Artist> artistInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic

    TestOutputTopic<String, TicketGenreStream.TicketGenre> outputTopic

    // outputs

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the MostProfitableCustomers topology (by reference)
        TicketGenreStream.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        // instantiate topics
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

        artistInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                TicketGenreStream.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TicketGenreStream.TICKET_GENRE_SERDE.deserializer()
        )
    }

    def 'cleanup'() {
        driver.close()
    }

    def "Customer Demographics"() {
        given: 'piping the artist through the stream'
        def artist1 = new Artist("artist-1", "John", "pop")
        def artist2 = new Artist("artist-2", "Jane", "rock")

        artistInputTopic.pipeKeyValueList([
                new KeyValue<String, Artist>(artist1.id(), artist1),
                new KeyValue<String, Artist>(artist2.id(), artist2),
        ])

        and: 'piping events through the stream'
        def event1_artist1 = EVENTS.generate("event-1", "artist-1", "venue-1", 10)
        def event2_artist1 = EVENTS.generate("event-2", "artist-1", "venue-1", 10)
        def event3_artist2 = EVENTS.generate("event-3", "artist-2", "venue-1", 10)
        def event4_artist2 = EVENTS.generate("event-4", "artist-2", "venue-1", 10)

        eventInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(event1_artist1.id(), event1_artist1),
                new KeyValue<String, Stream>(event2_artist1.id(), event2_artist1),
                new KeyValue<String, Stream>(event3_artist2.id(), event3_artist2),
                new KeyValue<String, Stream>(event4_artist2.id(), event4_artist2),
        ])

        and: 'piping tickets through the stream'
        def customer1_ticket1 = TICKETS.generate("customer-1", "event-1")
        def customer1_ticket2 = TICKETS.generate("customer-1", "event-2")
        def customer1_ticket3 = TICKETS.generate("customer-1", "event-4")
        def customer2_ticket1 = TICKETS.generate("customer-2", "event-1")
        def customer2_ticket2 = TICKETS.generate("customer-2", "event-4")

        ticketInputTopic.pipeKeyValueList([
                new KeyValue<String, Stream>(customer1_ticket1.id(), customer1_ticket1),
                new KeyValue<String, Stream>(customer1_ticket2.id(), customer1_ticket2),
                new KeyValue<String, Stream>(customer1_ticket3.id(), customer1_ticket3),
                new KeyValue<String, Stream>(customer2_ticket1.id(), customer2_ticket1),
                new KeyValue<String, Stream>(customer2_ticket2.id(), customer2_ticket2),
        ])

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: '6 records were received'
        outputRecords.size() == 5

        and: 'customer-1 has seen 2 out of 3 events for artist 1'
        outputRecords.get(0).value().genre == "pop"
        outputRecords.get(1).value().genre == "pop"
        outputRecords.get(2).value().genre == "rock"
        outputRecords.get(3).value().genre == "pop"
        outputRecords.get(4).value().genre == "rock"

    }
}
