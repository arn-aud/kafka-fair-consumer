import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka(count = 2, partitions = 1)
public class FairnessTest {

    static final int MAX_POLL_COUNT = 10;
    static final int MAX_POLL_RECORDS = 100;
    static final int QUIET_MESSAGE_COUNT = 4;
    static final int CHATTY_MESSAGE_COUNT = 4000;

    @Autowired
    EmbeddedKafkaBroker broker;

    @RepeatedTest(100)
    public void testFairness(RepetitionInfo repetitionInfo) throws Exception {

        // quiet-topic unique partition on first broker and chatty-topic unique partition on second broker
        NewTopic quietTopic = new NewTopic("quiet-" + repetitionInfo.getCurrentRepetition(), Map.of(0, List.of(1)));
        NewTopic chattyTopic = new NewTopic("chatty-" + repetitionInfo.getCurrentRepetition(), Map.of(0, List.of(0)));

        try (AdminClient adminClient = KafkaAdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                broker.getBrokersAsString()))) {

            adminClient.createTopics(List.of(quietTopic, chattyTopic)).all().get();
        }

        try (Producer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()),
                Serdes.String().serializer(), Serdes.String().serializer())) {
            // send just a few messages to a first topic
            for (int i = 0; i < QUIET_MESSAGE_COUNT; i++) {
                producer.send(new ProducerRecord<>(quietTopic.name(), "shh.."));
            }
            // send many messages to another topic
            for (int i = 0; i < CHATTY_MESSAGE_COUNT; i++) {
                producer.send(new ProducerRecord<>(chattyTopic.name(), "lalala!!!"));
            }
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                CommonClientConfigs.GROUP_ID_CONFIG, "group-" + repetitionInfo.getCurrentRepetition(),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                Serdes.String().deserializer(), Serdes.String().deserializer())) {

            consumer.subscribe(List.of(chattyTopic.name(), quietTopic.name()));

            for (int i = 0; i < MAX_POLL_COUNT; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.partitions().stream()
                        .anyMatch(topicPartition -> topicPartition.topic().equals(quietTopic.name()))) {
                    // some messages consumed from the "quiet" topic, it's fair
                    return;
                }
            }

            fail(String.format("no message consumed from quiet-topic after %d polls, it's not fair!", MAX_POLL_COUNT));
        }
    }

}
