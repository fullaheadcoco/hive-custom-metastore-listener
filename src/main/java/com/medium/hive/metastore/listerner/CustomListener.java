package com.medium.hive.metastore.listerner;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;

import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CustomListener extends MetaStoreEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomListener.class);
    private static final ObjectMapper objMapper = new ObjectMapper();

    public CustomListener(Configuration config) {
        super(config);
        logWithHeader("CustomListener", " created ");
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent event) throws MetaException {
        logWithHeader("onCreateDatabase", event.getDatabase());
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent event) throws MetaException {
        logWithHeader("onDropDatabase", event.getDatabase());
    }

    @Override
    public void onAlterDatabase(AlterDatabaseEvent event) throws MetaException {
        logWithHeader("onAlterDatabase:OldDatabase", event.getOldDatabase());
        logWithHeader("onAlterDatabase:NewDatabase", event.getNewDatabase());
    }

    @Override
    public void onCreateTable(CreateTableEvent event) {
        logWithHeader("onCreateTable", event.getTable());
    }

    @Override
    public void onDropTable(DropTableEvent event) {
        logWithHeader("onDropTable", event.getTable());
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        logWithHeader("onAlterTable:OldTable", event.getOldTable());
        logWithHeader("onAlterTable:NewTable", event.getNewTable());
    }

    @Override
    public void onAddPartition(AddPartitionEvent event) {
        logWithHeader("onAddPartition", event.getTable());
        StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(event.getPartitionIterator(), Spliterator.ORDERED), false)
                .forEach(p -> logWithHeader("onAddPartition", p));
    }

    @Override
    public void onDropPartition(DropPartitionEvent event) {
        logWithHeader("onDropPartition", event.getTable());
        logWithHeader("onDropPartition", event.getPartitionIterator());
        logWithHeader("onDropPartition", event.getDeleteData());
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent event) {
        logWithHeader("onAlterPartition:Table", event.getTable());
        logWithHeader("onAlterPartition:OldPartition", event.getOldPartition());
        logWithHeader("onAlterPartition:NewPartition", event.getNewPartition());
    }

    private void logWithHeader(String methodName, Object obj) {
        LOGGER.info("[CustomListener][Thread: " + Thread.currentThread().getName() + "] | [" + methodName + "]" + objToStr(obj));
    }

    private String objToStr(Object obj) {
        try {
            return objMapper.writeValueAsString(obj);
        } catch (IOException e) {
            LOGGER.error("Error on conversion", e);
        }
        return null;
    }
}
