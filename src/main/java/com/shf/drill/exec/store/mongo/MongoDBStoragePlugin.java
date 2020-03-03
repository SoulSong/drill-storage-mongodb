package com.shf.drill.exec.store.mongo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MongoDBStoragePlugin extends AbstractStoragePlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBStoragePlugin.class);

    private MongoDBStoragePluginConfig config;
    private DrillbitContext context;
    private MongoDBSchemaFactory schemaFactory;


    public MongoDBStoragePlugin(MongoDBStoragePluginConfig config, DrillbitContext inContext, String inName) {
        super(inContext, inName);
        this.config = config;
        this.context = inContext;
        this.schemaFactory = new MongoDBSchemaFactory(getName(), this);
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        MongoDBScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<MongoDBScanSpec>() {
        });
        LOGGER.info("getPhysicalScan {} {} {} {}", userName, selection, selection.getRoot(), spec);
        return new MongoDBGroupScan(userName, spec, config, null);
    }

    @Override
    public StoragePluginConfig getConfig() {
        return config;
    }

    @Override
    public DrillbitContext getContext() {
        return context;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

}
