package com.shf.drill.exec.store.mongo;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBScanSpec {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBScanSpec.class);

    /**
     * mql format as follows,it is a json_string：
     * {
     * "dbName": "***",
     * "collectionName": "***",
     * "aggs": [
     * "a",
     * "b"
     * ],
     * "projection": ""
     * }
     */
    private String mql;
//    private Spec spec;

    @JsonCreator
    public MongoDBScanSpec(@JsonProperty("mql") String mql) {
        LOGGER.info("mql : {}", mql);
        this.mql = mql;
    }

    public String getMql() {
        return mql;
    }

//    @JsonIgnore
//    public Spec getSpec() {
//        try {
//            spec = new ObjectMapper().readValue(this.mql, Spec.class);
//        } catch (IOException e) {
//            LOGGER.error("parse mql error : {}", e.getMessage());
//            e.printStackTrace();
//        }
//        return spec;
//    }
//
//    static class Spec {
//        private String dbName;
//        private String collectionName;
//        private List<String> aggs;
//        private String projection;
//
//        public String getDbName() {
//            return dbName;
//        }
//
//        public void setDbName(String dbName) {
//            this.dbName = dbName;
//        }
//
//        public String getCollectionName() {
//            return collectionName;
//        }
//
//        public void setCollectionName(String collectionName) {
//            this.collectionName = collectionName;
//        }
//
//        public List<String> getAggs() {
//            return aggs;
//        }
//
//        public void setAggs(List<String> aggs) {
//            this.aggs = aggs;
//        }
//
//        public String getProjection() {
//            return projection;
//        }
//
//        public void setProjection(String projection) {
//            this.projection = projection;
//        }
//    }
}
