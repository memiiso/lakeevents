package org.memiiso.cdcevents;

import io.quarkus.runtime.ShutdownEvent;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.aws.s3.S3Constants;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

@ApplicationScoped
//@Startup
public class CdcEventsServer extends RouteBuilder {
    private static final String DATABASE_READER =
            "debezium-postgres:localhost?"
                    + "databaseHostname={{database.hostname}}"
                    + "&databasePort={{database.port}}"
                    + "&databaseUser={{database.user}}"
                    + "&databasePassword={{database.password}}"
                    + "&databaseDbname={{database.dbname}}"
                    + "&databaseServerName={{database.hostname}}"
                    + "&schemaWhitelist={{database.schema}}"
                    + "&tableWhitelist={{table.whitelist}}"
                    + "&offsetStorageFileName=/tmp/offset.dat"
                    + "&pluginName=pgoutput";
    final Logger logger = LoggerFactory.getLogger(CdcEventsServer.class);
    @ConfigProperty(name = S3Constants.BUCKET_NAME, defaultValue = "test-bucket")
    public String S3_BUCKET_NAME;

    public void stop(@Observes ShutdownEvent event) {
        logger.warn("Stopping...");
    }

    @Override
    public void configure() {
        logger.error("Configuring...");
        //assert (DATABASE_READER.contains("localhost"));

        from(DATABASE_READER)
                .startupOrder(20)
                .routeId(CdcEventsServer.class.getName() + ".DatabaseReader")
                .log(LoggingLevel.ERROR, "Incoming message \nBODY: ${body} \nHEADERS: ${headers}");
    }
}