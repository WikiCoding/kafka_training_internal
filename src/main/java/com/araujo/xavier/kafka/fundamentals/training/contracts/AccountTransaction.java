package com.araujo.xavier.kafka.fundamentals.training.contracts;

public record AccountTransaction(String accountId, String transactionId, long timestamp, long value) {
}
