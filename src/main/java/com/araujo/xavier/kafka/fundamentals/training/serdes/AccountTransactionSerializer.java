package com.araujo.xavier.kafka.fundamentals.training.serdes;

import com.araujo.xavier.kafka.fundamentals.training.domain.AccountTransaction;
import com.araujo.xavier.kafka.fundamentals.training.contracts.Transaction;
import org.apache.kafka.common.serialization.Serializer;

public class AccountTransactionSerializer implements Serializer<AccountTransaction> {

    @Override
    public byte[] serialize(String s, AccountTransaction accountTransaction) {
        if (accountTransaction == null) return null;

        return Transaction.TransactionProto
                .newBuilder()
                .setAccountId(accountTransaction.accountId())
                .setTransactionId(accountTransaction.transactionId())
                .setValue(accountTransaction.value())
                .setTimestamp(accountTransaction.timestamp())
                .build().toByteArray();
    }
}