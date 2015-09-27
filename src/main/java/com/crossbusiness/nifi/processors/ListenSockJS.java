package com.crossbusiness.nifi.processors;

import com.crossbusiness.nifi.controllers.VertxServiceInterface;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
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

    public static final PropertyDescriptor INBOUND_ADDRESS = new PropertyDescriptor.Builder()
            .name("inbound address")
            .description("permitted matches for inbound (client->server) traffic")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTBOUND_ADDRESS  = new PropertyDescriptor.Builder()
            .name("outbound address")
            .description("permitted matches for outbound (server->client) traffic")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(PORT);
        properties.add(CONTEXT_PATH);
        properties.add(INBOUND_ADDRESS);
        properties.add(OUTBOUND_ADDRESS);
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



    private void createSockJSServer(final ProcessContext context) throws Exception {

        final ProcessorLog logger = getLogger();

        final int port = context.getProperty(PORT).asInteger();
        final String ContextPath = context.getProperty(CONTEXT_PATH).getValue();
        final VertxServiceInterface vertxService = context.getProperty(VERTX_SERVICE).asControllerService(VertxServiceInterface.class);

        Vertx vertx = vertxService.getVertx();
        Router router = Router.router(vertx);

        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);

        BridgeOptions options = new BridgeOptions();
        if (context.getProperty(INBOUND_ADDRESS).isSet()) {
            options.addInboundPermitted(new PermittedOptions().setAddress(context.getProperty(INBOUND_ADDRESS).getValue()));
        }
        if (context.getProperty(OUTBOUND_ADDRESS).isSet()) {
            options.addOutboundPermitted(new PermittedOptions().setAddress(context.getProperty(OUTBOUND_ADDRESS).getValue()));
        }

        sockJSHandler.bridge(options);

        router.route(ContextPath).handler(sockJSHandler);
        vertx.createHttpServer().requestHandler(router::accept).listen(port);
    }

    @OnScheduled
    public void startServer(final ProcessContext context) throws Exception {
        createSockJSServer(context);

    }

    @OnStopped
    public void shutdownSockJSServer() {
        //TODO
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile incoming = session.get();
        if ( incoming == null ) {
            return;
        }

        final ProcessorLog logger = getLogger();

        FlowFile outgoing = session.create();

        // TODO implement
        session.transfer(outgoing, REL_SUCCESS);
    }
}


