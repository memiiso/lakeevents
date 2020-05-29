package org.memiiso.lakeevents;

import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.PropertiesComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
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

    @Value( "${bucketNameOrArn}" )
    public String S3_BUCKET_NAME;

    @Override
    public void configure() {

        logger.info("Configure...");
        logger.info("S3 bucket name {}", S3_BUCKET_NAME);

        //final PropertiesComponent prop = getContext().getPropertiesComponent();
        //final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(prop.resolveProperty("broker.url").get());
        //final JmsComponent jmsComponent = JmsComponent.jmsComponentAutoAcknowledge(connectionFactory);
        //jmsComponent.setUsername(prop.resolveProperty("broker.user").get());
        //jmsComponent.setPassword(prop.resolveProperty("broker.password").get());
        //getContext().addComponent("jms", jmsComponent);

        //.multicast().streaming().parallelProcessing()
        //.stopOnException().to("direct:json-writer")
        //.end()
        //        .end()
        from(DATABASE_READER)
                .startupOrder(20)
                .routeId(LakeEventsServer.class.getName() + ".DatabaseReader")
                .log(LoggingLevel.ERROR, "Incoming message \nBODY: ${body} \nHEADERS: ${headers}")
                .autoStartup(true);
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