package com.crossbusiness.nifi.controllers;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.nifi.controller.ControllerService;

public interface VertxServiceInterface extends ControllerService {
    public Vertx getVertx();
    public EventBus getEventBus();
}
