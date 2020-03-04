package com.shf.drill.exec.store.mongo;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.drill.exec.vector.BaseValueVector.INITIAL_VALUE_ALLOCATION;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/2/28 22:01
 */
public class MongoDBRecordReader extends AbstractRecordReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBRecordReader.class);

    private FragmentContext fragmentContext;
    private JsonReader jsonReader;
    private VectorContainerWriter writer;
    private MongoDBSubScan subScan;
    private MongoDBScanSpec.Spec spec;

    private MongoClient mongoClient;
    private MongoCursor<Document> iterator;

    public MongoDBRecordReader(FragmentContext fragmentContext, MongoDBSubScan subScan, List<SchemaPath> projectedColumns) {
        this.fragmentContext = fragmentContext;
        this.subScan = subScan;
        spec = subScan.getScanSpec().getSpec();
        if (null == spec) {
            throw UserException
                    .validationError()
                    .message(
                            "spec must not be null.")
                    .addContext("Mql", subScan.getScanSpec().getMql())
                    .addContext("Plugin", subScan.getConfig().getPluginName())
                    .build(LOGGER);
        }

        // 此设置将影响jsonReader构造的返回体projection，即使select * 也仅包含如下设置列
        String projection = spec.getProjection();
        if (StringUtils.isNotEmpty(projection)) {
            final List<SchemaPath> newProjectedColumns = new ArrayList<>();
            Arrays.asList(projection.split(",")).forEach(projectColumn -> newProjectedColumns.add(SchemaPath.getSimplePath(projectColumn)));
            setColumns(newProjectedColumns);
        } else {
            setColumns(projectedColumns);
        }
    }

    private void init() {
        switch (subScan.getConfig().getMode()) {
            case "standalone":
                mongoClient = MongoClients.create("mongodb://" + subScan.getConfig().getConnections());
                break;
            case "replication":
                // todo
            case "shard":
                // todo
            default:
                throw UserException
                        .validationError()
                        .message(
                                "Mongodb connect mode error.")
                        .build(LOGGER);
        }
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        init();
        // 最终数据写入writer中
        this.writer = new VectorContainerWriter(output);
        this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
                .schemaPathColumns(Lists.newArrayList(getColumns()))
                .enableNanInf(true)
                .enableEscapeAnyChar(false)
                .build();

        if (null == spec.getDbName() || null == spec.getCollectionName() || CollectionUtils.isEmpty(spec.getAggs())) {
            throw UserException
                    .validationError()
                    .message(
                            "dbName and collectionName and aggs all must not be null.")
                    .addContext("Mql", subScan.getScanSpec().getMql())
                    .addContext("Plugin", subScan.getConfig().getPluginName())
                    .build(LOGGER);
        }

        LOGGER.info("DbName: {},CollectionName : {}", spec.getDbName(), spec.getCollectionName());
        List<Document> aggs = spec.getAggs().stream().map(agg -> {
            LOGGER.info(agg);
            return Document.parse(agg);
        }).collect(Collectors.toList());

        AggregateIterable<Document> iterable = mongoClient.getDatabase(spec.getDbName()).getCollection(spec.getCollectionName()).aggregate(aggs).allowDiskUse(true);
        iterator = iterable.iterator();
    }

    @Override
    public int next() {
        LOGGER.info("MongoDBRecordReader next");
        if (iterator == null || !iterator.hasNext()) {
            return 0;
        }
        writer.allocate();
        writer.reset();
        int docCount = 0;
        try {
            while (docCount < INITIAL_VALUE_ALLOCATION && iterator.hasNext()) {
                jsonReader.setSource(iterator.next().toJson().getBytes(Charsets.UTF_8));
                writer.setPosition(docCount);
                jsonReader.write(writer);
                docCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        writer.setValueCount(docCount);
        return docCount;
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

}
