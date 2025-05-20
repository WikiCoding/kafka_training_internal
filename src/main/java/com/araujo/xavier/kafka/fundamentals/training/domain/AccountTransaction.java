package com.araujo.xavier.kafka.fundamentals.training.domain;

public record AccountTransaction(String accountId, String transactionId, long timestamp, long value) {
}
