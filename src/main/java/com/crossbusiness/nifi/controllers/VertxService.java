package com.crossbusiness.nifi.controllers;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({"vertx", "sockJS", "listen", "non clustered"})
@SeeAlso(classNames = {"com.crossbusiness.nifi.processors.GetEventBus", "com.crossbusiness.nifi.processors.PublishEventBus", "com.crossbusiness.nifi.processors.SendEventBus"})
@CapabilityDescription("Starts SockJS/WebSocket Server." +
        "\nThe URL of the Service will be http://{hostname}:{port}/{contextPath}" +
        "\nProvides shared vertx, eventBus instances")
public class VertxService extends AbstractControllerService implements VertxServiceInterface {

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Listening Port")
            .description("the port to listen on for incoming connections")
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

    private List<PropertyDescriptor> properties;
    private volatile Vertx vertx = null;
    private volatile EventBus eventBus = null;
    private volatile HttpServer httpServer;

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(PORT);
        properties.add(CONTEXT_PATH);
        properties.add(INBOUND_ADDRESS_REGEX);
        properties.add(OUTBOUND_ADDRESS_REGEX);
        this.properties = Collections.unmodifiableList(properties);
        vertx = Vertx.vertx();
        eventBus = vertx.eventBus();
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private void createSockJSServer(final ConfigurationContext context)  throws InitializationException {

        final int port = context.getProperty(PORT).asInteger();
        final String ContextPath = context.getProperty(CONTEXT_PATH).getValue();

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

    @OnEnabled
    public void startServer(final ConfigurationContext context)  throws InitializationException {
        vertx = Vertx.vertx();
        eventBus = vertx.eventBus();
        getLogger().info("initializing vertx server...");
        createSockJSServer(context);
    }

    @OnDisabled
    public void shutdownServer() {
        if (httpServer != null) {
            httpServer.close(res -> {
                if (res.succeeded()) {
                    getLogger().info("server stopped");
                } else {
                    getLogger().error("server stop failed!");
                }
            });
        }
        if (eventBus != null) {
            eventBus.close(res -> {
                if (res.succeeded()) {
                    getLogger().info("eventBus closed");
                } else {
                    getLogger().error("eventBus close failed!");
                }
            });
        }
        if (vertx != null) {
            vertx.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        shutdownServer();
    }

    public Vertx getVertx() {
        return this.vertx;
    }

    public EventBus getEventBus() {
        return this.eventBus;
    }
}
