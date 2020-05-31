package org.memiiso.cdcevents;

import io.quarkus.test.junit.NativeImageTest;
import org.apache.camel.component.aws.s3.S3Constants;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

//@RunWith(SpringJUnit4ClassRunner.class)
//@ActiveProfiles("test")// load src/main/resources/application-test.properties
//@ContextConfiguration(classes = ConfigFileApplicationContextInitializer.class)
//@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD) // cleanup spring context because jms broker does not exit properly

/**
 * Native mode tests. In the native mode, the same tests will be executed as in the JVM mode because this class extends
 * {@link CdcEventsServerTest}.
 */
@NativeImageTest
public class CdcEventsServerIT extends CdcEventsServerTest {
    public final Logger logger = LoggerFactory.getLogger(CdcEventsServerIT.class);

    @Test
    public void endToEndTest() throws Exception {
        Thread.sleep(500000);
        assertThat(S3_BUCKET_NAME).isEqualTo("test-bucket");
        assertNotNull(ConfigProvider.getConfig().getValue(S3Constants.BUCKET_NAME, String.class));
    }
}