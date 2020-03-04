package com.shf.drill.exec.store.mongo;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.*;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/2/28 22:01
 */
public class MongoDBGroupScan extends AbstractGroupScan {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBGroupScan.class);

    private MongoDBScanSpec scanSpec;
    private MongoDBStoragePluginConfig config;
    private List<SchemaPath> columns;

    public MongoDBGroupScan(String userName, MongoDBScanSpec scanSpec,
                            MongoDBStoragePluginConfig config, List<SchemaPath> columns) {
        super(userName);
        this.scanSpec = scanSpec;
        this.config = config;
        this.columns = columns;
    }

    private MongoDBGroupScan(MongoDBGroupScan that) {
        super(that);
        this.scanSpec = that.scanSpec;
        this.config = that.config;
        this.columns = that.columns;
    }

    public MongoDBScanSpec getScanSpec() {
        return scanSpec;
    }

    public MongoDBStoragePluginConfig getConfig() {
        return config;
    }

    @Override
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        LOGGER.debug("MongoDBGroupScan applyAssignments");
    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        return new MongoDBSubScan(scanSpec, config, columns, getUserName());
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 1;
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        return new MongoDBGroupScan(this);
    }

    @Override
    public ScanStats getScanStats() {
        return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, 1, 1, (float) 10);
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        // selection columns from here
        MongoDBGroupScan clone = new MongoDBGroupScan(this);
        clone.columns = columns;
        return clone;
    }

    @Override
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }
}
