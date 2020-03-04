package com.shf.drill.exec.store.mongo;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/2/28 22:01
 */
public class MongoDBSubScan extends AbstractBase implements SubScan {
    private MongoDBScanSpec scanSpec;
    private MongoDBStoragePluginConfig config;
    private List<SchemaPath> columns;

    public MongoDBSubScan(MongoDBScanSpec scanSpec, MongoDBStoragePluginConfig config,
                          List<SchemaPath> columns, String userName) {
        super(userName);
        this.scanSpec = scanSpec;
        this.config = config;
        this.columns = columns;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return new MongoDBSubScan(scanSpec, config, columns, getUserName());
    }

    @Override
    public int getOperatorType() {
        return UserBitShared.CoreOperatorType.MONGO_SUB_SCAN.getNumber();
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
        return Collections.emptyIterator();
    }

    public List<SchemaPath> getColumns() {
        return columns;
    }

    public MongoDBScanSpec getScanSpec() {
        return scanSpec;
    }

    public MongoDBStoragePluginConfig getConfig() {
        return config;
    }

}
