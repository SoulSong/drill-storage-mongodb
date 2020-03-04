package com.shf.drill.exec.store.mongo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * 插件配置信息，可通过web控制台的storage配置TAB
 *
 * @author songhaifeng
 * @date 2020/2/28 22:01
 */
@JsonTypeName(MongoDBStoragePluginConfig.NAME)
public class MongoDBStoragePluginConfig extends StoragePluginConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBStoragePluginConfig.class);

    public static final String NAME = "mongodb";
    private String mode;
    private String connections;

    @JsonCreator
    public MongoDBStoragePluginConfig(@JsonProperty("mode") @NotNull String mode,
                                      @JsonProperty("connections") @NotNull String connections) {
        LOGGER.info("Initialize MongoDBStoragePluginConfig by the {} mode with {}.", mode, connections);
        this.connections = connections;
        this.mode = mode;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that == null || getClass() != that.getClass()) {
            return false;
        }
        MongoDBStoragePluginConfig t = (MongoDBStoragePluginConfig) that;
        return this.connections.equals(t.connections) && this.mode.equals(t.mode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connections, mode);
    }

    @Override
    public String toString() {
        return "MongoDBStoragePluginConfig{" +
                "mode='" + mode + '\'' +
                ", connections='" + connections + '\'' +
                '}';
    }

    public String getMode() {
        return mode;
    }

    public String getConnections() {
        return connections;
    }

    @JsonIgnore
    public String getPluginName() {
        return NAME.concat("-plugin");
    }
}
