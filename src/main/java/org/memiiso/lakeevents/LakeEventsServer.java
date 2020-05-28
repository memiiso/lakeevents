package org.memiiso.lakeevents;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.aws.s3.S3Constants;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

@ApplicationScoped
@Startup
public class LakeEventsServer extends RouteBuilder {
    final Logger logger = LoggerFactory.getLogger(LakeEventsServer.class);
    private static final String DATABASE_READER =
            "debezium-postgres:{{database.dbname}}?"
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

    @ConfigProperty(name = S3Constants.BUCKET_NAME, defaultValue = "test-bucket")
    public String S3_BUCKET_NAME;

    public void stop(@Observes ShutdownEvent event) {
        logger.warn("Stopping...");
    }

    RouteBuilder debeziumcdc = new RouteBuilder() {
        public void configure() {
            errorHandler(deadLetterChannel("mock:error"));
            from(DATABASE_READER)
                    .startupOrder(20)
                    .routeId(LakeEventsServer.class.getName() + ".DatabaseReader")
                    .log(LoggingLevel.ERROR, "Incoming message \nBODY: ${body} \nHEADERS: ${headers}");
        }
    };

    CamelContext context;

    public void manualstart() throws Exception {
        logger.error("Starting manualstart");
        context.addRoutes(debeziumcdc);
    }


    @Override
    public void configure() {
        context = getContext();
        /*
        logger.info("Configure...");
        logger.info("S3 bucket name {}", S3_BUCKET_NAME);
        final PropertiesComponent prop = getContext().getPropertiesComponent();
        /*
        final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(prop.resolveProperty("broker.url").get());
        final JmsComponent jmsComponent = JmsComponent.jmsComponentAutoAcknowledge(connectionFactory);
        jmsComponent.setUsername(prop.resolveProperty("broker.user").get());
        jmsComponent.setPassword(prop.resolveProperty("broker.password").get());
        getContext().addComponent("jms", jmsComponent);


        //.multicast().streaming().parallelProcessing()
        //.stopOnException().to("direct:json-writer")
        //.end()
        //        .end()
        RouteDefinition a = from(DATABASE_READER)
                .startupOrder(20)
                .routeId(LakeEventsServer.class.getName() + ".DatabaseReader")
                .log(LoggingLevel.ERROR, "Incoming message \nBODY: ${body} \nHEADERS: ${headers}");
/*
        from("direct:json-writer")
                .startupOrder(19)
                .routeId(LakeEventsServer.class.getName() + ".JMSQueeJSONWriter")
                .log(LoggingLevel.WARN, "JSON Sink message \nBODY: ${body} \nHEADERS: ${headers}")
                .marshal(new JsonDataFormat())
                .convertBodyTo(String.class)
                //You can include the optional queue: prefix, if you prefer:
                .to("jms:queue:CustomersJSON?disableReplyTo=true");
  /*
        from("jms:queue:CustomersJSON")
                .startupOrder(18)
                .routeId(LakeEventsServer.class.getName() + ".S3LakeWriter")
                .log(LoggingLevel.WARN, "JSON Sink message \nBODY: ${body} \nHEADERS: ${headers}")
                .convertBodyTo(String.class)
                .toD("aws2-s3://{{bucketNameOrArn}}?accessKey={{accessKey}}&secretKey={{secretKey}}&prefix={{prefix}}&" +
                        "overrideEndpoint={{overrideEndpoint}}&uriEndpointOverride={{uriEndpointOverride}}");

                /*
        // @TODO fix- enable avro after native build
        from("direct:avro-writer")
                .startupOrder(10)
                .routeId(LakeEventsServer.class.getName() + ".JMSQueeAvroWriter")
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

    from("aws-s3://bucket-name?deleteAfterRead=false&maxMessagesPerPoll=25&delay=5000")
            .log(LoggingLevel.INFO, "consuming", "Consumer Fired!")
            .idempotentConsumer(header("CamelAwsS3ETag"),
                    FileIdempotentRepository.fileIdempotentRepository(new File("target/file.data"), 250, 512000))
            .log(LoggingLevel.INFO, "Replay Message Sent to file:s3out ${in.header.CamelAwsS3Key}")
                .to("file:target/s3out?fileName=${in.header.CamelAwsS3Key}");

*/
    }
}