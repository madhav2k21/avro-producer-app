package com.techleads.app;

import java.util.Properties;

import com.techleads.app.avro.MyMessages;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.techleads.app.common.KafkaConstants;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
@Component
public class AvroProducerV1 implements CommandLineRunner {

	@Override
	public void run(String... args) throws Exception {
		
		Properties props=new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAPSERVERS);
		props.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConstants.ACKS_CONFIG_VAL);
		props.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaConstants.RETRIES_CONFIG_VAL);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMAREGISTRYSERVERS);


		KafkaProducer<String, MyMessages> kafkaProducer=new KafkaProducer<>(props);
		MyMessages message = MyMessages.newBuilder()
				.setMessage("Pay insurance")
				.setMsgId(101).build();

		ProducerRecord<String, MyMessages> producerRecord=new ProducerRecord<>(KafkaConstants.TOPIC, message);
		kafkaProducer.send(producerRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception==null){
					System.out.println("Message is published to kafka topic");
					System.out.println(metadata.toString()+" "+metadata.topic());
				}else{
					System.out.println("Some failure");
					System.out.println(exception.getMessage());
				}
			}
		});
		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
