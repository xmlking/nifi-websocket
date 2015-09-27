package com.crossbusiness.nifi.controllers;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;

@Tags({"vertx", "sockJS"})
@CapabilityDescription("shared vertx, eventBus instances")
public class VertxService extends AbstractControllerService implements VertxServiceInterface {

    private volatile Vertx vertx = null;
    private volatile EventBus eventBus = null;

    @Override //@OnEnabled
    public void init(ControllerServiceInitializationContext context)
            throws InitializationException {
        context.getLogger().info("initializing vertx...");
        vertx = Vertx.vertx();
        eventBus = vertx.eventBus();
    }


    @OnShutdown
    public void shutdown() {
        if (vertx != null) {
            vertx.close();
        }
    }

    public Vertx getVertx() {
        return this.vertx;
    }

    public EventBus getEventBus() {
        return this.eventBus;
    }
}
