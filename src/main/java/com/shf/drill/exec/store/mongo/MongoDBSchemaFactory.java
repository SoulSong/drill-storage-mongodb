package com.shf.drill.exec.store.mongo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class MongoDBSchemaFactory extends AbstractSchemaFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBSchemaFactory.class);

    private MongoDBStoragePlugin plugin;

    public MongoDBSchemaFactory(String schemaName, MongoDBStoragePlugin plugin) {
        super(schemaName);
        this.plugin = plugin;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        LOGGER.debug("registerSchema {}", getName());
        MongoDBSchema schema = new MongoDBSchema(getName());
        parent.add(getName(), schema);
    }

    public class MongoDBSchema extends AbstractSchema {
        private Set<String> tableNames = Sets.newHashSet();

        public MongoDBSchema(String name) {
            super(ImmutableList.<String>of(), name);
            tableNames.add("static");
        }

        @Override
        public String getTypeName() {
            return MongoDBStoragePluginConfig.NAME;
        }

        @Override
        public Set<String> getTableNames() {
            return tableNames;
        }

        @Override
        public Table getTable(String tableName) {
            LOGGER.info("MongoDBSchema.getTable {}", tableName);
            // will be pass to getPhysicalScan
            MongoDBScanSpec spec = new MongoDBScanSpec(tableName);
            return new DynamicDrillTable(plugin, getName(), null, spec);
        }

    }
}
