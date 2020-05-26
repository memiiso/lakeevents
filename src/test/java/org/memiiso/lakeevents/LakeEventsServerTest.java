package org.memiiso.lakeevents;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusTest
public class LakeEventsServerTest {
    static TestS3 s3server = new TestS3();
    static TestDatabase testDb = new TestDatabase();
    public final Logger logger = LoggerFactory.getLogger(LakeEventsServerTest.class);
    @InjectMock
    LakeEventsServer lakeEvents;

    @ConfigProperty(name = "bucketNameOrArn")
    String s3_name;

    @Test
    public void testLakeEventsService() {

        Assertions.assertNotNull(s3_name);
        Assertions.assertNotNull(lakeEvents);
    }

}