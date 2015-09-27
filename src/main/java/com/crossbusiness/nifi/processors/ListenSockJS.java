package com.crossbusiness.nifi.processors;

import com.crossbusiness.nifi.controllers.VertxServiceInterface;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@SeeAlso(PutEventBus.class)
@Tags({"ingest", "websocket", "sockJS", "ws", "wss", "listen"})
@WritesAttributes({
        @WritesAttribute(attribute = "eventbus.topic", description = "The name of the eventbus's topic from which the message was received")
})
@CapabilityDescription("Starts an WebSocket Server that is used to receive FlowFiles from remote sources. The URL of the Service will be http://{hostname}:{port}/wsListener")
public class ListenSockJS extends AbstractProcessor {

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received FlowFiles")
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Listening Port")
            .description("The Port to listen on for incoming connections")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTEXT_PATH  = new PropertyDescriptor.Builder()
            .name("URL prefix")
            .description("URL context path where clients can connect")
            .required(false)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .defaultValue("/eventbus/*")
            .build();

    public static final PropertyDescriptor INBOUND_ADDRESS_REGEX = new PropertyDescriptor.Builder()
            .name("inbound address regex")
            .description("permitted matches for inbound (client->server) traffic")
            .required(false)
            .defaultValue(".*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTBOUND_ADDRESS_REGEX  = new PropertyDescriptor.Builder()
            .name("outbound address regex")
            .description("permitted matches for outbound (server->client) traffic")
            .required(false)
            .defaultValue(".*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_DATA_RATE = new PropertyDescriptor.Builder()
            .name("Max Data to Receive per Second")
            .description("The maximum amount of data to receive per second; this allows the bandwidth to be throttled to a specified data rate; if not specified, the data rate is not throttled")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor VERTX_SERVICE = new PropertyDescriptor.Builder()
            .name("Vertx Service").description("The Controller Service that is used to obtain vert.x and eventBus instances")
            .identifiesControllerService(VertxServiceInterface.class).required(true).build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.relationships = Collections.singleton(REL_SUCCESS);

        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(PORT);
        properties.add(CONTEXT_PATH);
        properties.add(INBOUND_ADDRESS_REGEX);
        properties.add(OUTBOUND_ADDRESS_REGEX);
        properties.add(MAX_DATA_RATE);
        properties.add(VERTX_SERVICE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    private MessageConsumer<String> consumer;
    private HttpServer httpServer;

    private void createSockJSServer(final ProcessContext context) throws Exception {

        final ProcessorLog logger = getLogger();

        final int port = context.getProperty(PORT).asInteger();
        final String ContextPath = context.getProperty(CONTEXT_PATH).getValue();
        final VertxServiceInterface vertxService = context.getProperty(VERTX_SERVICE).asControllerService(VertxServiceInterface.class);

        Vertx vertx = vertxService.getVertx();
        Router router = Router.router(vertx);

        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);

        BridgeOptions options = new BridgeOptions();
        if (context.getProperty(INBOUND_ADDRESS_REGEX).isSet()) {
            options.addInboundPermitted(new PermittedOptions().setAddressRegex(context.getProperty(INBOUND_ADDRESS_REGEX).getValue()));
        }
        if (context.getProperty(OUTBOUND_ADDRESS_REGEX).isSet()) {
            options.addOutboundPermitted(new PermittedOptions().setAddressRegex(context.getProperty(OUTBOUND_ADDRESS_REGEX).getValue()));
        }

        sockJSHandler.bridge(options);

        router.route(ContextPath).handler(sockJSHandler);
        httpServer = vertx.createHttpServer().requestHandler(router::accept).listen(port);
    }

    @OnScheduled
    public void startServer(final ProcessContext context) throws Exception {
        createSockJSServer(context);

        final VertxServiceInterface vertxService = context.getProperty(VERTX_SERVICE).asControllerService(VertxServiceInterface.class);
        final EventBus eb = vertxService.getEventBus();
        consumer = eb.consumer("draw");
        consumer.handler(message -> {
            System.out.println("I have received a message: " + message.body().toString());
        });

    }

    @OnStopped
    public void shutdownSockJSServer() {
        //TODO
        consumer.unregister(res -> {
            if (res.succeeded()) {
                System.out.println("The handler un-registration has reached all nodes");
            } else {
                System.out.println("Un-registration failed!");
            }
        });
        httpServer.close(res -> {
            if (res.succeeded()) {
                System.out.println("Server Stopped");
            } else {
                System.out.println("Server stop failed!");
            }
        });
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();

        FlowFile outgoing = session.create();

        // TODO implement
        session.transfer(outgoing, REL_SUCCESS);
    }
}


