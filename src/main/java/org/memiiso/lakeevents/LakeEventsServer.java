package org.memiiso.lakeevents;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.aws.s3.S3Constants;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.model.dataformat.JsonDataFormat;
import org.apache.camel.spi.PropertiesComponent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;

//@Service
@ApplicationScoped
@Startup
@Service
@Component
public class LakeEventsServer extends RouteBuilder {
    private static final String S3_LAKE_WRITER = "direct:json-writer";
    private static final String S3_LAKE_AVRO_WRITER = "direct:avro-writer";
    final Logger logger = LoggerFactory.getLogger(LakeEventsServer.class);
    private static final String DATABASE_READER =
            "debezium-postgres:{{database.hostname}}?"
                    + "databaseHostname={{database.hostname}}"
                    + "&databasePort={{database.port}}"
                    + "&databaseUser={{database.user}}"
                    + "&databasePassword={{database.password}}"
                    + "&databaseDbname={{database.dbname}}"
                    + "&databaseServerName={{database.hostname}}"
                    + "&schemaWhitelist={{database.schema}}"
                    + "&tableWhitelist={{database.schema}}.customers"
                    + "&offsetStorageFileName=/tmp/offset.dat"
                    + "&pluginName=pgoutput";
    @Inject
    public CamelContext camelContext;
    @ConfigProperty(name = S3Constants.BUCKET_NAME, defaultValue = "test-bucket")
    public String S3_BUCKET_NAME;

    //private void typeConverterSetup() {
    //    getContext().getTypeConverterRegistry()
    //            .addTypeConverter(Customer.class, Struct.class, new CustomerConverter());
    //}

    @SuppressWarnings("unchecked")
    @PostConstruct
    public void start() {
        logger.info("Starting...");
        logger.warn("Camel contex name is {}", camelContext.getName());
    }

    public void stop(@Observes ShutdownEvent event) {
        logger.warn("Stopping...");
    }

    @Override
    public void configure() {
        logger.warn("Configure...");
        //from("timer://testTimer").log(LoggingLevel.INFO,"This is testTimer logging");
        //this.jmsComponentSetup();

        from(DATABASE_READER)
                .routeId(LakeEventsServer.class.getName() + ".DatabaseReader")
                .log(LoggingLevel.WARN, "Incoming message \nBODY: ${body} \nHEADERS: ${headers}")
                .multicast().streaming().parallelProcessing()
                .stopOnException().to("direct:json-writer", "direct:avro-writer")
                .end()
                .end();

        from("direct:json-writer")
                .routeId(LakeEventsServer.class.getName() + ".S3LakeWriter")
                .log(LoggingLevel.WARN, "JSON Sink message \nBODY: ${body} \nHEADERS: ${headers}")
                .marshal(new JsonDataFormat())
                .convertBodyTo(String.class)
                .to("jms:queue:CustomersJSON?disableReplyTo=true");

        // @TODO fix- enable avro after native build
        from("direct:avro-writer")
                .routeId(LakeEventsServer.class.getName() + ".S3LakeAvroWriter")
                .log(LoggingLevel.WARN, "AVRO Sink message \nBODY: ${body} \nHEADERS: ${headers}")
                .marshal(new JsonDataFormat())
                .convertBodyTo(String.class)
                .to("jms:queue:CustomersAvro?disableReplyTo=true");
    }

    //private void typeConverterSetup() {
    ////    getContext().getTypeConverterRegistry()
    //    camelContext.getTypeConverterRegistry()
    //            .addTypeConverter(Customer.class, Struct.class, new CustomerConverter());
    // }
    private void jmsComponentSetup() {
        logger.error(getContext().getPropertiesComponent().getLocations().toString());
        logger.error(getContext().getComponentNames().toString());
        final PropertiesComponent prop = getContext().getPropertiesComponent();
        final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(prop.resolveProperty("broker.url").get());
        final JmsComponent jmsComponent = JmsComponent.jmsComponentAutoAcknowledge(connectionFactory);
        jmsComponent.setUsername(prop.resolveProperty("broker.user").get());
        jmsComponent.setPassword(prop.resolveProperty("broker.password").get());
        getContext().addComponent("jms", jmsComponent);
    }

/*
    from("aws-s3://bucket-name?deleteAfterRead=false&maxMessagesPerPoll=25&delay=5000")
            .log(LoggingLevel.INFO, "consuming", "Consumer Fired!")
            .idempotentConsumer(header("CamelAwsS3ETag"),
                    FileIdempotentRepository.fileIdempotentRepository(new File("target/file.data"), 250, 512000))
            .log(LoggingLevel.INFO, "Replay Message Sent to file:s3out ${in.header.CamelAwsS3Key}")
                .to("file:target/s3out?fileName=${in.header.CamelAwsS3Key}");
*/
}
