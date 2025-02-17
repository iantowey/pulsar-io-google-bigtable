package com.eg.dataeng;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.demo.dataeng.BigTableSink;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;

public class BigTableSinkMain {
    private static final Logger log = LoggerFactory.getLogger(BigTableSinkMain.class);

    public static void main(String[] args) throws Exception {
        SinkConfig sc = new SinkConfig();

        sc.setTenant("public");
        sc.setNamespace("default");
        sc.setName("google-bigtable-sink");
        sc.setInputs(Collections.singleton("persistent://public/default/test-bigtable-sink"));
        sc.setClassName(BigTableSink.class.getName());
        sc.setParallelism(1);
        sc.setAutoAck(true);
        sc.setConfigs(new HashMap<>());

        String creds = new String(Files.readAllBytes(Paths.get("/home/itowey/eg/projects/DEDEV/pulsar-io-google-bigtable/terraform/bigtable-test-sa.json")));

        sc.getConfigs().put("credentialJsonString", creds);
        sc.getConfigs().put("projectId", "eg-gcp-dataengcore-prod");
        sc.getConfigs().put("instanceId", "test-instance");
        sc.getConfigs().put("cluster", "test-cluster");
        sc.getConfigs().put("tableName","taxonomy-propensity-bt-table");
        sc.getConfigs().put("tableColumnFamilies", "coupon-offer-cols");
        sc.getConfigs().put("batchSize", 5000.0);
        sc.getConfigs().put("appProfileId", "test-cluster");

        String s = "{\"pubsubProjectId\":\"eg-gcp-dataengcore-prod\",\"pubsubTopicId\":\"my-pubsub-topic\",\"pubsubCredential\": \"{\\\"type\\\":\\\"service_account\\\",\\\"project_id\\\":\\\"eg-gcp-dataengcore-prod\\\",\\\"private_key_id\\\":\\\"c7858a024219c4a6f3788c1e06f3fe0586746fb5\\\",\\\"private_key\\\":\\\"-----BEGIN PRIVATE KEY-----\\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCp7RR+KflCqGVk\\nvU7CCY0gvN/2OexNIJZ2M0Ssh9J8rlfHVvMLZVwoV+oaQj2cKDck47+65T9pxzlh\\nUDRmvqv7D9+PPFUrUZYcE5RjEMV3fqWx33V6QWUiFf3za0UAVOL+Nh6tJzeEVVsY\\nPHEPEPJrEG7Laduj+9dYJkUCQTN193ht7wdPistkN0oMGxJCcli+M1LLywCkDrAS\\ncTnokSa/TQWWmymOh08rlYCJ+BarRTBvLsQJma2kxv+iPbjvlI5gAPn8xQdC8/Td\\n6l/tHLQfw/0CY3I392tfUoqLU4kctYZC/VnJVR6JSI6BaVJTl+8hCLFIur5Yf2A7\\nYvKulflHAgMBAAECggEAUFhgfa0EBjAQTRszn4yblbSooshNgkRb0ES6fmd0L27R\\n41ep8KP6+Lpk3wieD/V16XPkZo7Ztn60E160gDY8pCejuZq53JMlil35pgO6kp8n\\n4lw5tFYyZvUGD/AS/q6ka7WUOKQX3HLtcR63CHwnW1c4Q+boKve7oRrrOz8wAWD9\\nysHx3JMtxMQBvsgdXK8W7HjYCVoEQztQqGEkpiw6PUGRp984rSaJYX+3D6C5uKFS\\n8Zsi772K/7x8B9LHoc6bWe9YLlIkMqrHMgKnC3zkR8Z3RUzEEstzXUtXyfTi6LiI\\ncXQUvas5EJ6mmTAaQ+W0E1w1WiBQdG+OXCoxHOOvRQKBgQDYUxMBC73jd8dOcrlG\\nFkkqvJrF/RBiVFCEdZlL1NmsHhxhvIZpuHHPeeByuYbo4pVZCWoV6Tp+h4pqxN7d\\nwabVChKJ14WyLgKU5CPCaVFbTIM5KZ5dfXY9Qycab07A2ZuUycm8QsrmUVN0Ultx\\nKX3bwQnRoUTS8hykRgRID5QzfQKBgQDJF37AmKlARXsJVeEavoH8FQU2S/V62wzM\\nkmxiBem2kgHT2/2OGo17oRfAmsHXGSo2dCejktNKtpW2JKnvpGUiA/XQhYXlm7YS\\nVSxEZeJsR4e0i2GEA16zKoJ+7pq2EMTuoKQt65f28lQfgFg9ol8brmkRaOZbQMeb\\nh9on0VlzEwKBgQDDiuyt0FhYywgtgIS7tx9yc0SQ3f22dLLH2am1o3UUIa/UJV93\\nJ1QxkODzs4Mg9ti7wEfWAJpwj8LuWoF/ATL2doFlep2PAAozXrp4XU0+cz5XKKjP\\np75/CDnLqnwM1WfOqk0iUVopsa/3gQ7JjxEG2Usv5FBbQqHjWKs8HGEB3QKBgQDD\\n4n82XX+BfC0+Csocozc7t748BgI6iucM4BVz5w6jKddd6Q41PuhTZbkdhaMNRRY5\\ntSxnyr7IKNPtegmPxzQ8zl7FoUutPuE9OnrEpfrKP2OSG7QCRMYbAW3c99D9SZiM\\nWy+TC8wcYjJP/VX3tBOxg1QWAXljqtbGSq/X2baowQKBgQDMea2SMBRGc6uYjAmk\\n4XkLoIqn2rPDUrkXFy5Rh/zzCk68LSh8ZzdqgY0AmU0TnlyPsSiOexlWbiE6xgCZ\\nFhqXCT4VI2JxcywOeIn2dWa0O6wbcCw/Naq9qXgCElqILNybC/1K8meUFs25GbFL\\nMaWK2B+vheL6h4m/e2i6MAavaw==\\n-----END PRIVATE KEY-----\\n\\\",\\\"client_email\\\":\\\"pubsub-test@eg-gcp-dataengcore-prod.iam.gserviceaccount.com\\\",\\\"client_id\\\":\\\"114972577596803239373\\\",\\\"auth_uri\\\":\\\"https://accounts.google.com/o/oauth2/auth\\\",\\\"token_uri\\\":\\\"https://oauth2.googleapis.com/token\\\",\\\"auth_provider_x509_cert_url\\\":\\\"https://www.googleapis.com/oauth2/v1/certs\\\",\\\"client_x509_cert_url\\\":\\\"https://www.googleapis.com/robot/v1/metadata/x509/pubsub-test%40eg-gcp-dataengcore-prod.iam.gserviceaccount.com\\\",\\\"universe_domain\\\":\\\"googleapis.com\\\"}\"}";

        System.out.println(new ObjectMapper().writeValueAsString(sc));

        LocalRunner localRunner = LocalRunner.builder().sinkConfig(sc).build();
        localRunner.start(true);
        log.info("Sink runner setup complete.");
    }
}
