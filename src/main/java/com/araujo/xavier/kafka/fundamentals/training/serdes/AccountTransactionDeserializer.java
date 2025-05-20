package com.araujo.xavier.kafka.fundamentals.training.serdes;

import com.araujo.xavier.kafka.fundamentals.training.contracts.AccountTransaction;
import com.araujo.xavier.kafka.fundamentals.training.contracts.Transaction;
import org.apache.kafka.common.serialization.Deserializer;

public class AccountTransactionDeserializer implements Deserializer<AccountTransaction> {
    @Override
    public AccountTransaction deserialize(String s, byte[] bytes) {
        if (bytes == null) return null;

        try {
            Transaction.TransactionProto message = Transaction.TransactionProto.newBuilder()
                    .mergeFrom(bytes)
                    .build();

            return new AccountTransaction(
                    message.getAccountId(),
                    message.getTransactionId(),
                    message.getValue(),
                    message.getTimestamp()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }
}
