package com.dcaiti.mosaic.app.tse.processors;

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import com.dcaiti.mosaic.app.fxd.messages.FcdUpdateMessage;
import com.dcaiti.mosaic.app.tse.data.DatabaseAccess;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import org.eclipse.mosaic.lib.database.Database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This processor can be used to store all received FCD Records within the {@link FcdDataStorage}.
 * This will be executed every {@link #triggerInterval}.
 */
public class FcdWriterProcessor extends TimeBasedProcessor<FcdRecord, FcdUpdateMessage> implements DatabaseAccess {

    private static final String IDENTIFIER = createIdentifier(FcdWriterProcessor.class);

    private FcdDataStorage fcdDataStorage;
    private final Map<String, Collection<FcdRecord>> recordBuffer = new HashMap<>();

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public void withDataStorage(Database networkDatabase, FcdDataStorage fcdDataStorage) {
        this.fcdDataStorage = fcdDataStorage;
    }

    @Override
    public void shutdown(long shutdownTime) {
        logger.info("Final Persistence of FCD Records for {} vehicles.", recordBuffer.keySet().size());
        persistRecords();
    }

    @Override
    public void handleUpdate(FcdUpdateMessage update) {
        String vehicleId = update.getRouting().getSource().getSourceName();
        // here we don't profit from the sorted map, so we just store them as a collection right away
        Collection<FcdRecord> recordList = new ArrayList<>(update.getRecords().values());
        if (!recordBuffer.containsKey(vehicleId)) {
            recordBuffer.put(vehicleId, recordList);
        } else {
            recordBuffer.get(vehicleId).addAll(recordList);
        }
    }

    @Override
    public void triggerEvent(long eventTime) {
        logger.info("Persisting FCD Records for {} vehicles.", recordBuffer.keySet().size());
        persistRecords();
    }

    private void persistRecords() {
        fcdDataStorage.insertFcdRecords(recordBuffer);
        recordBuffer.clear();
    }
}
