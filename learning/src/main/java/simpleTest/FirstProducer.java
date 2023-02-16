package simpleTest;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;


public class FirstProducer {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        String serverUrl = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        System.out.println("Produce records until 'End' is given as input: ");
        int counter = 0;
        String recordInput = scanner.nextLine();
        while (!"End".equals(recordInput)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", ("key" + counter++), recordInput);
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("Successfully received the details as: \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset" + recordMetadata.offset() + "\n" +
                            "Timestamp" + recordMetadata.timestamp());
                } else {
                    System.out.println("Can't produce,getting error" + e);

                }
            });

            recordInput = scanner.nextLine();
        }

        producer.flush();
        producer.close();
    }
}
