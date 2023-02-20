package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import producer.ProducerWithCallback;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerShutdown {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());
    private static final String TOPIC = "demo-topic";
    private static final String CONSUMER_GROUP = "simple-cg";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown detected!");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }));

        try {
            consumer.subscribe(List.of(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Partition: {} | Key: {} | Offset: {}",
                            record.partition(), record.key(), record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Consumer is shutting down!");
        } catch (Exception e) {
            logger.error("Unexpected exception! ", e);
        } finally {
            consumer.close();
        }
    }
}
