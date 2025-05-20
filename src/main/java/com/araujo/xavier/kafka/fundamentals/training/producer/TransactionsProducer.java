package com.araujo.xavier.kafka.fundamentals.training.producer;

import com.araujo.xavier.kafka.fundamentals.training.contracts.AccountTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

@Slf4j
public class TransactionsProducer {
	private final String topic;
	private final Properties properties;
	private final List<AccountTransaction> transactions;

	public TransactionsProducer(String topic, Properties properties) {
		this.topic = topic;
		this.properties = properties;

		final var numberOfTransactions = 10;
		Random random = new Random();

		this.transactions = new ArrayList<>(numberOfTransactions);

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
	}

	private void start() {
		try (final KafkaProducer<String, AccountTransaction> producer = new KafkaProducer<>(properties)) {
			for (AccountTransaction transaction : transactions) {
				producer.send(
						new ProducerRecord<>(topic, transaction.accountId(), transaction),
						(metadata, e) -> {
							if (e != null) {
								log.error("Error sending message. Details: {}", e.getMessage(), e);
							} else {
								log.info("Transaction {} successfully sent to {}", transaction.accountId(), metadata);
							}
						}
				);
			}
		} catch (Exception e) {
			log.error("Error in transaction producer. Details: {}", e.getMessage(), e);
			cleanUp();
			throw e;
		}
		finally {
			closeAnyOtherResourcesSafely();
		}
	}

	private void cleanUp() {
		// do any cleanup needed, rollback any transaction...
	}

	private void closeAnyOtherResourcesSafely() {
		// close any other resources
	}
}
