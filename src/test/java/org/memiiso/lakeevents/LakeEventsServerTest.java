package org.memiiso.lakeevents;

import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@CamelSpringBootTest
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
//@ContextConfiguration(classes = Application.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ComponentScan("org.memiiso.lakeevents")
public class LakeEventsServerTest extends CamelTestSupport {
    static TestS3 s3server = new TestS3();
    static TestDatabase testDb = new TestDatabase();
    public final Logger logger = LoggerFactory.getLogger(LakeEventsServerTest.class);

    @Value( "${bucketNameOrArn}" )
    public String S3_BUCKET_NAME;
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
    public void testLakeEventsService() throws Exception {

        logger.warn("running testLakeEventsService");
        logger.warn("running testLakeEventsService {}",context.getName());

        assertEquals(S3_BUCKET_NAME,"test-bucket");

        Assertions.assertNotNull(S3_BUCKET_NAME);
        logger.warn("started LakeEventsServer");
        //Assertions.assertNotNull(s);
        Thread.sleep(500);
        assertEquals(S3_BUCKET_NAME,"test-bucket");

        /*
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
            if (objects.size() >= 2) {
                System.out.println(objects.toString());
            }
            return (objects.size() >= 2);
        });
        */
    }

}