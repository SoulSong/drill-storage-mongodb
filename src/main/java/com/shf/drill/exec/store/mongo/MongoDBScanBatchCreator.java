package com.shf.drill.exec.store.mongo;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/2/28 22:01
 */
public class MongoDBScanBatchCreator implements BatchCreator<MongoDBSubScan> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBScanBatchCreator.class);

    @Override
    public CloseableRecordBatch getBatch(ExecutorFragmentContext context, MongoDBSubScan config,
                                         List<RecordBatch> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        List<RecordReader> readers = Lists.newArrayList();
        List<SchemaPath> columns = config.getColumns() == null ? GroupScan.ALL_COLUMNS : config.getColumns();
        readers.add(new MongoDBRecordReader(context, config, columns));
        return new ScanBatch(config, context, readers);
    }
}
