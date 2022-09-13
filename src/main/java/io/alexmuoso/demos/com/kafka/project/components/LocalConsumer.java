package io.alexmuoso.demos.com.kafka.project.components;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalConsumer {

	private static final Logger log = LoggerFactory.getLogger(LocalConsumer.class);
	public static void consumeMessages() {
		log.info("Building Kafka Consumer");
    	log.info("Configure");
    	Properties prop = new Properties();
    	prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    	prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "second_group");
    	prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest means it read from the beginning 
    	
    	log.info("Create consumer");
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
    	
    	final Thread mainThread = Thread.currentThread();
    	Runtime.getRuntime().addShutdownHook(new Thread() {
    		public void run() {
    			log.info("Detected a shutdown, lets exit by calling consumer.wakeup()");
    			consumer.wakeup();
    			try {
    				mainThread.join();
    			}catch (InterruptedException e) {
    				e.printStackTrace();
    			}
    		}
    	});
    	
		try {
	       	log.info("Subscribe to the topic");
	    	consumer.subscribe(Arrays.asList("first_topic"));
	    	
	    	log.info("Poll");
	    	while(true) {
	    		log.info("POOLING");
	    		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));//poll kafka and get many record as you can, if there are not wait 100ms then go to the next line of code. record will be null
	    		for (ConsumerRecord<String, String> record : records) {
					log.info("Key {} : value : {)" , record.key() , record.value());
					log.info("Key {} : value :{) offset : {}" , record.key() , record.value() ,record.offset());
				}
	    	}
	    	
		}catch (WakeupException e) {
			log.error("WakeupException",e);	
		}catch (Exception e) {
			log.error("Unexpected exception",e);	
		}finally {
			consumer.close();
			log.info("Consumer close"); // it will also commit the offset
		}
	}
}
