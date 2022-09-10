package io.alexmuoso.demos.com.kafka.project;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo 
{
	private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
	
    public static void main( String[] args )
    {
    	//producer
    	logger.info("Configure");
    	Properties prop = new Properties();
    	prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    	prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	logger.info("Create Producer");
    	Producer<String, String> producer = new KafkaProducer<>(prop);
    	
    	logger.info("Create Record");
    	ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "First_message");
    	
    	logger.info("Send Record");
    	producer.send(record);
    	
    	logger.info("Flush");
    	producer.flush();
    	
    	logger.info("Close");
    	producer.close();
    }
}
