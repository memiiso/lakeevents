package org.memiiso.lakeevents;

import io.quarkus.test.junit.QuarkusTest;
import org.apache.camel.component.aws.s3.S3Constants;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class LakeEventsServerTest {
    static TestS3 s3server = new TestS3();
    static TestDatabase testDb = new TestDatabase();
    public final Logger logger = LoggerFactory.getLogger(LakeEventsServerTest.class);
    @ConfigProperty(name = S3Constants.BUCKET_NAME, defaultValue = "test-bucket")
    public String S3_BUCKET_NAME;
    @ConfigProperty(name = "bucketNameOrArn")
    String s3_name;

    {
        logger.warn("creating LakeEventsServer");
        //lakeEvents = new LakeEventsServer();
    }

    @BeforeAll
    public static void startContainers() {
        s3server.start();
        testDb.start();
    }

    @AfterAll
    public static void stop() {
        if (s3server != null) {
            s3server.stop();
        }
        if (testDb != null) {
            testDb.stop();
        }
    }

    @Test
    public void testLakeEventsService() throws InterruptedException, URISyntaxException {
        logger.warn("running testLakeEventsService");
        Assertions.assertNotNull(S3_BUCKET_NAME);
        LakeEventsServer lakeEvents = new LakeEventsServer();
        Assertions.assertNotNull(lakeEvents);
        Thread.sleep(500);
        assertThat(S3_BUCKET_NAME).isEqualTo("test-bucket");
        assertNotNull(ConfigProvider.getConfig().getValue(S3Constants.BUCKET_NAME, String.class));

        //ProfileCredentialsProvider pcred = ProfileCredentialsProvider.create("default");
        AwsBasicCredentials c = AwsBasicCredentials.create("test", "testtest");
        StaticCredentialsProvider pcred = StaticCredentialsProvider.create(c);
        S3Client s3client = S3Client.builder()
                .credentialsProvider(pcred)
                .endpointOverride(new java.net.URI("http://" + s3server.getContainerIpAddress() + ':' + s3server.getMappedPort()))
                .build();
        s3client.createBucket(CreateBucketRequest.builder().bucket("test-bucket").build());
        org.fest.assertions.Assertions.assertThat(s3client.listBuckets().toString().contains("test-bucket"));

        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket("test-bucket")
                    .build();
            ListObjectsResponse res = s3client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            System.out.println(objects.toString());
            if (objects.size() >= 2) {
                System.out.println(objects.toString());
            }
            return (objects.size() >= 2);
        });

    }

}