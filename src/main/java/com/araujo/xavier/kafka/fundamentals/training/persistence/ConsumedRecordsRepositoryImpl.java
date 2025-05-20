package com.araujo.xavier.kafka.fundamentals.training.persistence;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

@Component
@Slf4j
@Primary
public class ConsumedRecordsRepositoryImpl implements ConsumedRecordsRepository {
    private static final Path REGISTER_FILE = Paths.get("processed_accounts.txt");

    public ConsumedRecordsRepositoryImpl() {
        if (!Files.exists(REGISTER_FILE)) {
            try {
                Files.createFile(REGISTER_FILE);
                log.info("Register file created: {}", REGISTER_FILE);
            } catch (IOException e) {
                log.error("Failed to create register file: {}", REGISTER_FILE, e);
            }
        }
    }

    @Override
    public void register(String accountId) {
        try {
            Files.writeString(
                    REGISTER_FILE,
                    accountId + System.lineSeparator(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND
            );
            log.debug("Registered new accountId: {}", accountId);
        } catch (IOException e) {
            log.error("Failed to register accountId: {}", accountId, e);
        }
    }

    @Override
    public Set<String> loadProcessedAccountIds() {
        Set<String> processedAccountIds = new HashSet<>();

        try (Stream<String> lines = Files.lines(REGISTER_FILE)) {
            lines.forEach(processedAccountIds::add);
//            log.info("Loaded {} processed account IDs from {}", processedAccountIds.size(), REGISTER_FILE);
            return processedAccountIds;
        } catch (IOException e) {
            log.error("Failed to load processed account IDs from {}", REGISTER_FILE, e);
            return Set.of();
        }
    }
}
