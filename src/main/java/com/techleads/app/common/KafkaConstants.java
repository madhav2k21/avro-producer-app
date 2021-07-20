package com.techleads.app.common;

public interface KafkaConstants {
	String BOOTSTRAPSERVERS ="localhost:9092,localhost:9093";
	String TOPIC= "MA_MSG";
	String GROUP_ID="messagesGroup";
	String APP_ID="messagesID123";
	String SCHEMAREGISTRYSERVERS = "http://localhost:8081";
	String ACKS_CONFIG_VAL="1";
	String RETRIES_CONFIG_VAL="10";

}
