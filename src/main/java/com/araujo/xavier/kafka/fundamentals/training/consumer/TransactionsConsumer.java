package com.araujo.xavier.kafka.fundamentals.training.consumer;

import com.araujo.xavier.kafka.fundamentals.training.domain.AccountTransaction;
import com.araujo.xavier.kafka.fundamentals.training.persistence.ConsumedRecordsRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

@Slf4j
@AllArgsConstructor
public class TransactionsConsumer {
    private final String topic;
    private final Properties properties;
    private final ConsumedRecordsRepository repository;

    private void start() {
        try (KafkaConsumer<String, AccountTransaction> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, AccountTransaction> records = consumer.poll(Duration.ofMillis(100));
                processBatch(records);
                consumer.commitSync();
            }
        } catch (Exception e) {
            cleanUp(e);
            throw new RuntimeException(e);
        } finally {
            smoothCloseOffOtherResources();
        }
    }

    private void processBatch(ConsumerRecords<String, AccountTransaction> records) {
        Set<String> processedAccountIds = repository.loadProcessedAccountIds();

        // if we didn't want to log anything, this could make use of saveAll(records) to store as a batch instead
        for (final ConsumerRecord<String, AccountTransaction> record : records) {
            String accountId = record.value().accountId();

            if (processedAccountIds.add(accountId)) {
                repository.register(accountId);
                log.info("Processed transaction {}", accountId);
                continue;
            }

            log.warn("Already processed transaction {}. Skipped.", accountId);
        }
    }

    private void cleanUp(Exception e) {
        // Any cleanup process and retries to spin up the consumer again if it crashes
    }

    private void smoothCloseOffOtherResources() {
        // we may want to add logic for smooth closing of any resources
    }
}
