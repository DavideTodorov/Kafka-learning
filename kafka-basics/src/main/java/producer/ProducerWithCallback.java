package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    private static final String TOPIC = "demo-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "200");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int j = 0; j < 20; j++) {
            for (int i = 0; i < 50; i++) {
                String value = String.valueOf(i);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, value);
                producer.send(producerRecord, (recordMetadata, exception) -> {
                    if (exception == null) {
                        logger.info("Message sent successfully!\n Partition: {}\n", recordMetadata.partition());

                    } else {
                        logger.error("Exception when sending message ", exception);
                    }
                });
            }
        }

        producer.close();
    }
}
