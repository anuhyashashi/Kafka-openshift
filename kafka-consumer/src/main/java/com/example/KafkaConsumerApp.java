// package com.example;

// import org.apache.kafka.clients.consumer.*;
// import org.apache.kafka.common.serialization.StringDeserializer;
// import org.apache.kafka.common.errors.WakeupException;
// import java.time.Duration;
// import java.util.*;

// public class KafkaConsumerApp {
//     public static void main(String[] args) {
//         Properties props = new Properties();

//       
//         props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
//         props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
//         props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//         props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//         props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // read from beginning

//         KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

//         try {
//             System.out.println("[INFO] Connecting to Kafka broker at kafka:9092...");
//             consumer.subscribe(Collections.singletonList("test-topic"));
//             System.out.println("[INFO] Subscribed to topic: test-topic");

//             while (true) {
//                 System.out.println("[DEBUG] Polling for new messages...");
//                 ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

//                 if (records.isEmpty()) {
//                     System.out.println("[DEBUG] No messages received in this poll.");
//                 }

//                 for (ConsumerRecord<String, String> record : records) {
//                     System.out.printf("[RECEIVED] Topic: %s | Partition: %d | Offset: %d | Value: %s%n",
//                             record.topic(), record.partition(), record.offset(), record.value());
//                 }
//             }

//         } catch (WakeupException we) {
//             System.err.println("[ERROR] WakeupException occurred. " + we.getMessage());
//         } catch (Exception e) {
//             System.err.println("[ERROR] Unexpected exception: " + e.getMessage());
//             e.printStackTrace();
//         } finally {
//             System.out.println("[INFO] Closing consumer.");
//             consumer.close();
//         }
//     }
// }

package com.example;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {
    public static void main(String[] args) {
        // Load Mongo URI
        String mongoUri = "mongodb://mongo:27017/messages";

        // Set up MongoDB
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        MongoDatabase database = mongoClient.getDatabase("messages");
        MongoCollection<Document> collection = database.getCollection("kafka_messages");

        // Set up Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList("test-topic"));
            System.out.println("[INFO] Subscribed to topic: test-topic");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();

                    System.out.printf("[RECEIVED] Topic: %s | Offset: %d | Message: %s%n",
                            record.topic(), record.offset(), message);

                    Document doc = new Document("message", message)
                            .append("topic", record.topic())
                            .append("offset", record.offset())
                            .append("timestamp", System.currentTimeMillis());

                    collection.insertOne(doc);
                    System.out.println("[INFO] Inserted message into MongoDB.");
                }
            }

        } catch (WakeupException we) {
            System.err.println("[ERROR] WakeupException: " + we.getMessage());
        } catch (Exception e) {
            System.err.println("[ERROR] Exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            mongoClient.close();
            System.out.println("[INFO] Closed Kafka and MongoDB clients.");
        }
    }
}
