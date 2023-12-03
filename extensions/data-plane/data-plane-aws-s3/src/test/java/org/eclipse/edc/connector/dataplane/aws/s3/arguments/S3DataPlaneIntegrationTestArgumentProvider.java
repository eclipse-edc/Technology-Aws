package org.eclipse.edc.connector.dataplane.aws.s3.arguments;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.UUID;
import java.util.stream.Stream;

public class S3DataPlaneIntegrationTestArgumentProvider implements ArgumentsProvider {
    private final String keyPrefix = UUID.randomUUID().toString();

    private final String keyPrefixDelimiter = "/";

    private final String body = UUID.randomUUID().toString();

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
                Arguments.of(
                        Named.of("Simple file transfer",
                                S3DataPlaneIntegrationTestArgument.Builder.newInstance()
                                        .addKey(generateRandomKey())
                                        .body(body)
                                        .build()
                        )
                ),
                Arguments.of(
                        Named.of("Multiple file transfer",
                                S3DataPlaneIntegrationTestArgument.Builder.newInstance()
                                        .addKey(generateRandomKeyWithPrefix())
                                        .addKey(generateRandomKeyWithPrefix())
                                        .addKey(generateRandomKeyWithPrefix())
                                        .keyPrefix(keyPrefix)
                                        .keyPrefixDelimiter(keyPrefixDelimiter)
                                        .body(body)
                                        .build()
                        )
                )
        );
    }

    private String generateRandomKey() {
        return UUID.randomUUID().toString();
    }

    private String generateRandomKeyWithPrefix() {
        return keyPrefix + keyPrefixDelimiter + generateRandomKey();
    }
}