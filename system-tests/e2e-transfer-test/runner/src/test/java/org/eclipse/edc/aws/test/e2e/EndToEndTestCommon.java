package org.eclipse.edc.aws.test.e2e;

import io.restassured.http.ContentType;
import org.apache.http.HttpStatus;
import org.eclipse.edc.junit.testfixtures.TestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.connector.controlplane.contract.spi.types.negotiation.ContractNegotiationStates.FINALIZED;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.COMPLETED;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;

public class EndToEndTestCommon {
    
    private static final String PROVIDER_MANAGEMENT_API = "http://localhost:8181/management/v3";
    private static final String CONSUMER_MANAGEMENT_API = "http://localhost:9191/management/v3";
    
    //TODO duplicated with test class
    private static final String TEST_RESOURCES = "system-tests/e2e-transfer-test/runner/src/test/resources/";
    
    private EndToEndTestCommon() {}
    
    public static void createProviderSecret(String id, String value) {
        var requestBody = readFile(TEST_RESOURCES + "secret.json")
                .replace("{secret-id}", id)
                .replace("{secret-value}", value);
        post(PROVIDER_MANAGEMENT_API + "/secrets", requestBody);
    }
    
    public static void createConsumerSecret(String id, String value) {
        var requestBody = readFile(TEST_RESOURCES + "secret.json")
                .replace("{secret-id}", id)
                .replace("{secret-value}", value);
        post(CONSUMER_MANAGEMENT_API + "/secrets", requestBody);
    }
    
    public static void createAsset(String endpointOverride) {
        var requestBody = readFile(TEST_RESOURCES + "asset.json")
                .replace("{endpoint-override}", endpointOverride);
        post(PROVIDER_MANAGEMENT_API + "/assets", requestBody);
    }
    
    public static void createPolicy() {
        post(PROVIDER_MANAGEMENT_API + "/policydefinitions", readFile(TEST_RESOURCES + "policy.json"));
    }
    
    public static void createContractDefinition() {
        post(PROVIDER_MANAGEMENT_API + "/contractdefinitions", readFile(TEST_RESOURCES + "contract-definition.json"));
    }
    
    public static String initiateNegotiation() {
        return post(CONSUMER_MANAGEMENT_API + "/contractnegotiations", readFile(TEST_RESOURCES + "initiate-negotiation.json"), "@id");
    }
    
    public static void waitForFinalization(String negotiationId) {
        Callable<String> getNegotiationState = () -> get(CONSUMER_MANAGEMENT_API + "/contractnegotiations/" + negotiationId, "state");
        
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1))
                .until(getNegotiationState, FINALIZED.name()::equals);
    }
    
    public static String getAgreementId(String negotiationId) {
        return get(CONSUMER_MANAGEMENT_API + "/contractnegotiations/" + negotiationId, "contractAgreementId");
    }
    
    public static String initiateTransfer(String agreementId, String endpointOverride) {
        var requestBody = readFile(TEST_RESOURCES + "initiate-transfer.json")
                .replace("{agreement-id}", agreementId)
                .replace("{endpoint-override}", endpointOverride);
        return post(CONSUMER_MANAGEMENT_API + "/transferprocesses", requestBody, "@id");
    }
    
    public static void waitForCompletion(String transferProcessId) {
        Callable<String> getTransferState = () -> get(CONSUMER_MANAGEMENT_API + "/transferprocesses/" + transferProcessId, "state");
    
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(5))
                .until(getTransferState, COMPLETED.name()::equals);
    }
    
    public static String get(String url, String jsonPath) {
        return given()
                .headers("X-Api-Key", "password")
                .contentType(ContentType.JSON)
                .when()
                .get(url)
                .then()
                .log().ifError()
                .statusCode(HttpStatus.SC_OK)
                .body(jsonPath, not(emptyString()))
                .extract()
                .jsonPath()
                .get(jsonPath);
    }
    
    private static void post(String url, String requestBody) {
        given()
                .headers("X-Api-Key", "password")
                .contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post(url)
                .then()
                .log().ifError()
                .statusCode(HttpStatus.SC_OK);
    }
    
    private static String post(String url, String requestBody, String jsonPath) {
        return given()
                .headers("X-Api-Key", "password")
                .contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post(url)
                .then()
                .log().ifError()
                .statusCode(HttpStatus.SC_OK)
                .body(jsonPath, not(emptyString()))
                .extract()
                .jsonPath()
                .get(jsonPath);
    }
    
    private static String readFile(String relativePath) {
        var absolutePath = new File(TestUtils.findBuildRoot(), relativePath).getAbsolutePath();
        try {
            return Files.readString(Path.of(absolutePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
