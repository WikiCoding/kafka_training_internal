package com.araujo.xavier.kafka.fundamentals.training.persistence;

import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public interface ConsumedRecordsRepository {
    void register(String accountIds);
    Set<String> loadProcessedAccountIds();
}
