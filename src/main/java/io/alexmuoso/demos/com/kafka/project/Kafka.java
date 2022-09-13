package io.alexmuoso.demos.com.kafka.project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.alexmuoso.demos.com.kafka.project.components.LocalConsumer;
import io.alexmuoso.demos.com.kafka.project.components.LocalProducer;

public class Kafka 
{
	private static final Logger logger = LoggerFactory.getLogger(Kafka.class.getSimpleName());
	
    public static void main( String[] args ) {
    	LocalProducer producerTemplate = new LocalProducer();
    	producerTemplate.produceMessage("msg1");
    	
    	LocalConsumer consumer = new LocalConsumer();
    	consumer.consumeMessages();

    }
}
