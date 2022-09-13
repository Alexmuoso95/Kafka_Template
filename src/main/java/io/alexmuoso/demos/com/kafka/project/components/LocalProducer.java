package io.alexmuoso.demos.com.kafka.project.components;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalProducer {

	private static final Logger log = LoggerFactory.getLogger(LocalProducer.class);
	public static void produceMessage(String msg) {
		log.info("Building Kafka Producer");
    	log.info("Configure");
    	Properties prop = new Properties();
    	prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    	prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	log.info("Create Producer");
    	Producer<String, String> producer = new KafkaProducer<>(prop);
    	
    	log.info("Create Record");
//    	ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "First_message");
    	ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "key_1", "First_message");

    	log.info("Send Record");
//    	producer.send(record);
    	producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				//every time we send a message
				if(exception == null) {
					log.info("Sucess record published topic : {} , key : {} , partition : {} , offset  : {} , timestamp : {}" ,
				    metadata.topic(), record.key() , metadata.partition() ,metadata.offset() ,metadata.timestamp());
				}else {
					log.error("Something went wrong {}", exception.getMessage(), exception);
				}
			}
		});

    	log.info("Flush");
    	producer.flush();
    	
    	log.info("Close");
    	producer.close();
	}
}
