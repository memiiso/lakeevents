package org.memiiso.lakeevents;

import io.quarkus.test.junit.NativeImageTest;
import org.apache.camel.component.aws.s3.S3Constants;
import org.eclipse.microprofile.config.ConfigProvider;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

//@RunWith(SpringJUnit4ClassRunner.class)
//@ActiveProfiles("test")// load src/main/resources/application-test.properties
//@ContextConfiguration(classes = ConfigFileApplicationContextInitializer.class)
//@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD) // cleanup spring context because jms broker does not exit properly

/**
 * Native mode tests. In the native mode, the same tests will be executed as in the JVM mode because this class extends
 * {@link LakeEventsServerTest}.
 */
@NativeImageTest
public class LakeEventsServerIT extends LakeEventsServerTest {

    static TestS3 s3server = new TestS3();
    static TestDatabase testDb = new TestDatabase();
    public final Logger logger = LoggerFactory.getLogger(LakeEventsServerIT.class);

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
    public void endToEndTest() throws Exception {
        Thread.sleep(500000);
        assertThat(lakeEvents.S3_BUCKET_NAME).isEqualTo("test-bucket");
        assertNotNull(ConfigProvider.getConfig().getValue(S3Constants.BUCKET_NAME, String.class));

        ProfileCredentialsProvider pcred = ProfileCredentialsProvider.create("default");
        S3Client s3client = S3Client.builder()
                .credentialsProvider(pcred)
                .endpointOverride(new java.net.URI("http://" + s3server.getContainerIpAddress() + ':' + s3server.getMappedPort()))
                .build();
        s3client.createBucket(CreateBucketRequest.builder().bucket("test-bucket").build());
        Assertions.assertThat(s3client.listBuckets().toString().contains("test-bucket"));

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
        Assertions.assertThat(objects.size() >= 2);
    }
}