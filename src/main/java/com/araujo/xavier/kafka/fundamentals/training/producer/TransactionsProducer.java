package com.araujo.xavier.kafka.fundamentals.training.producer;

import com.araujo.xavier.kafka.fundamentals.training.domain.AccountTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

@Slf4j
public class TransactionsProducer {
	private final String topic;
	private final Properties properties;

	public TransactionsProducer(String topic, Properties properties) {
		this.topic = topic;
		this.properties = properties;
	}

	public void start(List<AccountTransaction> transactions) {
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
