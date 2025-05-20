package com.araujo.xavier.kafka.fundamentals.training;

import com.araujo.xavier.kafka.fundamentals.training.domain.AccountTransaction;
import com.araujo.xavier.kafka.fundamentals.training.producer.TransactionsProducer;
import com.araujo.xavier.kafka.fundamentals.training.serdes.AccountTransactionSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import java.io.IOException;
import java.util.*;

@SpringBootApplication
@Profile("producer")
public class TransactionsProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(TransactionsProducerApplication.class, args);
	}

	@Bean
	public TransactionsProducer transactionsProducer(@Value("${kafka.topics.transactions}") String topic) throws IOException {
		Properties properties = new Properties();
		properties.load(TransactionsProducerApplication.class.getClassLoader().getResourceAsStream("producer.properties"));
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AccountTransactionSerializer.class);
		properties.put(ProducerConfig.ACKS_CONFIG, "all"); // acks from leader and replicas
//		properties.put(ProducerConfig.ACKS_CONFIG, "0"); // no acks
//		properties.put(ProducerConfig.ACKS_CONFIG, "1"); // acks just from leader
		properties.put(ProducerConfig.RETRIES_CONFIG, 3);
		properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // retry after 1sec
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // setting a max batch size
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); /* Kafka ensures that retries do not result in
		* duplicate messages, even if the producer retries due to a transient error. It guarantees Exactly Once from
		the Producer perspective. For this to properly work we have to have acks set to "all" */
		return new TransactionsProducer(topic, properties);
	}

	@Bean
	public CommandLineRunner commandLineRunner(TransactionsProducer transactionsProducer) {
		return args -> {
			final var numberOfTransactions = 10;
			Random random = new Random();

			List<AccountTransaction> transactions = new ArrayList<>();

			for (int i = 0; i < numberOfTransactions; i++) {
				transactions.add(
						new AccountTransaction(
								UUID.randomUUID().toString(),
								UUID.randomUUID().toString(),
								100 + random.nextInt(10_000),
								1_000 + random.nextInt(100_000)
						)
				);
			}

			transactions.add(
					new AccountTransaction(
							"99a314cf-332b-4671-8239-deb273d47f27",
							UUID.randomUUID().toString(),
							100 + random.nextInt(10_000),
							1_000 + random.nextInt(100_000)
					)
			);

			transactionsProducer.start(transactions);
		};
	}
}
