package org.improving.workshop.utopia

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.improving.workshop.Streams
import org.improving.workshop.utopia.ticket_demographics.*
import org.msse.demo.mockdata.customer.profile.Customer
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.CUSTOMERS

class AgedCustomersSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Customer> customerInputTopic
    TestOutputTopic<String, AgedCustomer> outputTopic

    // outputs

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the MostProfitableCustomers topology (by reference)
        AgedCustomerStream.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        // instantiate topics
        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                AgedCustomerStream.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                AgedCustomerStream.SERDE_AGED_CUSTOMER_JSON.deserializer()
        )
    }

    def 'cleanup'() {
        driver.close()
    }

    def "Customer Demographics"() {
        given: 'piping the customers through the stream'
        def customer1 = new Customer("customer-1", "PREMIUM", "M", "Joe", "Steven", "James", "JSJ", "", "", "1959-01-20", "2022-01-02")
        def customer2 = new Customer("customer-2", "PREMIUM", "F", "Jannet", "Jo", "James", "JJJ", "", "", "1958-01-20", "2022-01-02")
        def customer3 = new Customer("customer-3", "PREMIUM", "M", "Jannet", "Jo", "James", "JJJ", "", "", "1993-01-20", "2022-01-02")
        def customer4 = new Customer("customer-4", "PREMIUM", "F", "Jannet", "Jo", "James", "JJJ", "", "", "1994-01-20", "2022-01-02")
        def customer5 = new Customer("customer-5", "PREMIUM", "M", "Jannet", "Jo", "James", "JJJ", "", "", "1995-01-20", "2022-01-02")
        def customer6 = new Customer("customer-6", "PREMIUM", "F", "Jannet", "Jo", "James", "JJJ", "", "", "2000-01-20", "2022-01-02")
        customerInputTopic.pipeKeyValueList([
                new KeyValue<String, Customer>(customer1.id(), customer1),
                new KeyValue<String, Customer>(customer2.id(), customer2),
                new KeyValue<String, Customer>(customer3.id(), customer3),
                new KeyValue<String, Customer>(customer4.id(), customer4),
                new KeyValue<String, Customer>(customer5.id(), customer5),
                new KeyValue<String, Customer>(customer6.id(), customer6),
        ])

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: '6 records were received'
        outputRecords.size() == 6

        and: 'customer-1 has seen 2 out of 3 events for artist 1'
        outputRecords.get(0).value().age() == 65
        outputRecords.get(0).value().ageRange() == "Old"
        outputRecords.get(1).value().age() == 66
        outputRecords.get(1).value().ageRange() == "Old"
        outputRecords.get(2).value().age() == 31
        outputRecords.get(2).value().ageRange() == "Middle"
        outputRecords.get(3).value().age() == 30
        outputRecords.get(3).value().ageRange() == "Middle"
        outputRecords.get(4).value().age() == 29
        outputRecords.get(4).value().ageRange() == "Young"
        outputRecords.get(5).value().age() == 24
        outputRecords.get(5).value().ageRange() == "Young"

    }
}
