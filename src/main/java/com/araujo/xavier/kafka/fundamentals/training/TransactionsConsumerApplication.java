package com.araujo.xavier.kafka.fundamentals.training;

import com.araujo.xavier.kafka.fundamentals.training.consumer.TransactionsConsumer;
import com.araujo.xavier.kafka.fundamentals.training.persistence.ConsumedRecordsRepository;
import com.araujo.xavier.kafka.fundamentals.training.serdes.AccountTransactionDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import java.io.IOException;
import java.util.Properties;

@SpringBootApplication
@Profile("consumer")
public class TransactionsConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(TransactionsConsumerApplication.class, args);
	}

	@Bean(initMethod = "start")
	public TransactionsConsumer transactionsConsumer(
			@Value("${kafka.topics.transactions}") String topic,
			ConsumedRecordsRepository repository) throws IOException {
		Properties properties = new Properties();
		properties.load(TransactionsConsumerApplication.class.getClassLoader().getResourceAsStream("consumer.properties"));
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AccountTransactionDeserializer.class);
//		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); /* to start consuming from beginning if
//		you delete the consumer group. To avoid duplicate messages requires an idempotency strategy, as it's implemented!
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // to start consuming from the latest offset
		
		return new TransactionsConsumer(topic, properties, repository);
	}
}
