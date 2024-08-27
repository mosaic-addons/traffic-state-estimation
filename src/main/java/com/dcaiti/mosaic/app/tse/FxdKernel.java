/*
 * Copyright (c) 2021 Fraunhofer FOKUS and others. All rights reserved.
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contact: mosaic@fokus.fraunhofer.de
 */

package com.dcaiti.mosaic.app.tse;

import com.dcaiti.mosaic.app.fxd.data.FxdRecord;
import com.dcaiti.mosaic.app.fxd.data.FxdTraversal;
import com.dcaiti.mosaic.app.fxd.messages.FxdUpdateMessage;
import com.dcaiti.mosaic.app.tse.config.CFxdReceiverApp;
import com.dcaiti.mosaic.app.tse.events.ExpiredUnitRemovalEvent;
import com.dcaiti.mosaic.app.tse.processors.FxdProcessor;
import com.dcaiti.mosaic.app.tse.processors.MessageBasedProcessor;
import com.dcaiti.mosaic.app.tse.processors.TimeBasedProcessor;
import com.dcaiti.mosaic.app.tse.processors.TraversalBasedProcessor;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.lib.util.scheduling.EventManager;
import org.eclipse.mosaic.lib.util.scheduling.EventProcessor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The {@link FxdKernel} and all its inheriting classes handle traversal recognition, event management, initializing and execution of
 * the {@link FxdProcessor FxdProcessors}.
 *
 * @param <RecordT>    Record type (extension of {@link FxdRecord}) containing all spatio-temporal information that units
 *                     periodically transmit to the server
 * @param <TraversalT> Traversal type (extension of {@link FxdTraversal}) containing the traversal of a single connection consisting of a
 *                     list of {@link RecordT Records}.
 * @param <UpdateT>    Updated type (extension of {@link FxdUpdateMessage}) representing the actual messages send by units.
 *                     The most generic implementation contains a collection of records sampled by a unit.
 * @param <ConfigT>    Type of the configuration (extension of {@link CFxdReceiverApp}) in its generic form this class contains
 *                     configuration parameters for the expired unit removal and the configured {@link FxdProcessor FxdProcessors}.
 */
public abstract class FxdKernel<
        RecordT extends FxdRecord,
        TraversalT extends FxdTraversal<RecordT, TraversalT>,
        UpdateT extends FxdUpdateMessage<RecordT>,
        ConfigT extends CFxdReceiverApp<RecordT, TraversalT, UpdateT>>
        implements EventProcessor {
    private final EventManager eventManager;
    protected final UnitLogger logger;
    /**
     * Field holding the configuration to
     * See: {@link CFxdReceiverApp}
     */
    protected final ConfigT config;
    /**
     * A Map containing all known senders and all received records, that haven't been processed yet.
     * The value of this map is as {@link SortedMap}, which uses the time of record creation as key, which allows for easy
     * garbage collection.
     */
    private final Map<String, SortedMap<Long, RecordT>> recordBuffer = new HashMap<>();
    /**
     * A map to store all traversed connections of a sender.
     */
    private final Map<String, List<String>> connectionsBuffer = new HashMap<>();
    /**
     * A look back to store the last record of a previous traversal if that should be required in a processor.
     */
    private final Map<String, RecordT> recordLookBack = new HashMap<>();
    protected final List<FxdProcessor> allProcessors;
    private final List<TraversalBasedProcessor<RecordT, TraversalT>> traversalBasedProcessors = new ArrayList<>();
    private final List<MessageBasedProcessor> messageBasedProcessors = new ArrayList<>();
    /**
     * A registry holding all time based processors with identifiable keys (i.e., their class names) for event handling.
     * Note that this implicates that only one of each {@link TimeBasedProcessor} type can be used in the same simulation.
     */
    private final Map<String, TimeBasedProcessor<RecordT, UpdateT>> timeBasedProcessors = new HashMap<>();
    private long lastRemovalTime = 0;
    private long oldestAllowedRecordTime = 0;

    /**
     * Constructor for the {@link FxdKernel} initializing the {@link FxdProcessor FxdProcessors} and setting the event manager and logger.
     *
     * @param eventManager used for managing event handling for configured {@link FxdProcessor FxdProcessors} and
     *                     the removal of expired units of the {@link #recordBuffer}
     * @param logger       for logging purposes, gets forwarded to {@link FxdProcessor FxdProcessors}
     * @param config       configuration of the application
     */
    public FxdKernel(EventManager eventManager, UnitLogger logger, ConfigT config) {
        this.eventManager = eventManager;
        this.logger = logger;
        this.config = config;
        if (config.traversalBasedProcessors != null) {
            this.traversalBasedProcessors.addAll(config.traversalBasedProcessors);
        }
        if (config.timeBasedProcessors != null) {
            // create registry of time-based processors
            config.timeBasedProcessors.forEach(processor -> {
                if (processor != null && processor.triggerInterval >= 0) { // validate that processor has a legal trigger interval
                    this.timeBasedProcessors.put(processor.getIdentifier(), processor);
                }
            });
        }
        if (config.messageBasedProcessors != null) {
            messageBasedProcessors.addAll(config.messageBasedProcessors);
        }
        allProcessors = combineAllFxdProcessors();
        allProcessors.forEach(processor -> processor.initialize(logger));
        // initial event scheduling
        timeBasedProcessors.forEach((name, processor) -> scheduleEventForTimeBasedProcessor(processor));
        scheduleUnitRemoval();
    }

    private List<FxdProcessor> combineAllFxdProcessors() {
        final List<FxdProcessor> allProcessors;
        allProcessors = new ArrayList<>();
        allProcessors.addAll(timeBasedProcessors.values());
        allProcessors.addAll(traversalBasedProcessors);
        allProcessors.addAll(messageBasedProcessors);
        return allProcessors;
    }

    /**
     * This is method is the link between the kernel and the application, handling the updates for both the
     * {@link TimeBasedProcessor TimeBasedProcessors} and the {@link TraversalBasedProcessor TraversalBasedProcessors}.
     *
     * @param update the received update
     */
    public void processUpdate(UpdateT update) {
        // time-based processors will keep their own representation of
        timeBasedProcessors.values().forEach(processor -> processor.handleUpdate(update));
        processUpdateForTraversals(update);
        additionalProcessingOfUpdate(update); // implementation-specific handling of update
        if (update.isFinal()) { // remove sender after it was processed for last traversal
            String unitName = update.getRouting().getSource().getSourceName();
            recordBuffer.remove(unitName);
            recordLookBack.remove(unitName);
            connectionsBuffer.remove(unitName);
        }
    }

    private void processUpdateForTraversals(FxdUpdateMessage<RecordT> fxdUpdateMessage) {
        String senderId = fxdUpdateMessage.getRouting().getSource().getSourceName();
        List<String> senderConnections = extractTraversedConnections(senderId, fxdUpdateMessage.getRecords());
        SortedMap<Long, RecordT> senderRecords = enqueueRecords(senderId, fxdUpdateMessage.getRecords());
        // if there is more than one connection, we know there is a completed traversal
        while (senderConnections.size() > 1) { // we cannot be sure that only on connection has been traversed in the last update
            TraversalT traversal = extractTraversal(senderId, senderConnections.get(0), senderRecords);
            logger.debug("Handling traversal for vehicle: {} on connection {}", senderId, traversal.getConnectionId());
            triggerTraversalBasedProcessors(senderId, traversal);
            senderConnections.remove(0); // traversal has been processed
        }
    }

    /**
     * This method extracts a sorted list in order of a vehicle's route containing all non-processed connection traversals.
     *
     * @param senderId id of the sender
     * @param records  the newly received records
     * @return an ordered (by route) list of traversed connections
     */
    private List<String> extractTraversedConnections(String senderId, SortedMap<Long, RecordT> records) {
        if (!connectionsBuffer.containsKey(senderId)) { // on first record of a vehicle add it
            connectionsBuffer.put(senderId, Lists.newArrayList());
        }
        // scan records for all traversed connections
        List<String> senderConnections = connectionsBuffer.get(senderId);
        String prevConnection = senderConnections.isEmpty() ? null : senderConnections.get(senderConnections.size() - 1);
        for (RecordT currentRecord : records.values()) {
            if (!currentRecord.getConnectionId().equals(prevConnection)) {
                senderConnections.add(currentRecord.getConnectionId());
                prevConnection = currentRecord.getConnectionId();
            }
        }
        return senderConnections;
    }

    /**
     * Method that extracts traversal from received {@link RecordT records} and bundles them in a {@link TraversalT traversal} object.
     *
     * @param senderId            the id of the unit that has been recognized to traverse a connection
     * @param currentConnectionId the traversed connection
     * @param senderRecords       all relevant {@link RecordT records} for the traversals
     * @return the extracted {@link TraversalT traversal}
     */
    private TraversalT extractTraversal(String senderId, String currentConnectionId, SortedMap<Long, RecordT> senderRecords) {
        // extract all entries on the earliest connection traversal
        List<RecordT> traversalRecords = new ArrayList<>();
        Long currentKey = senderRecords.firstKey();
        while (senderRecords.get(currentKey).getConnectionId().equals(currentConnectionId)) {
            traversalRecords.add(senderRecords.get(currentKey));
            senderRecords.remove(currentKey);
            currentKey = senderRecords.firstKey();
        }
        RecordT previousRecord = recordLookBack.get(senderId);
        RecordT followingRecord = senderRecords.get(currentKey);
        recordLookBack.put(senderId, traversalRecords.get(traversalRecords.size() - 1)); // save last record of traversal for next traversal
        return createTraversal(traversalRecords, previousRecord, followingRecord);
    }

    private void triggerTraversalBasedProcessors(String senderId, TraversalT traversal) {
        // handle traversal for all traversal based processors
        traversalBasedProcessors.forEach(processor -> processor.onConnectionTraversal(senderId, traversal.copy()));
    }

    private SortedMap<Long, RecordT> enqueueRecords(String senderId, SortedMap<Long, RecordT> records) {
        if (recordBuffer.containsKey(senderId)) {
            recordBuffer.get(senderId).putAll(records);
        } else {
            recordBuffer.putIfAbsent(senderId, records);
        }
        return recordBuffer.get(senderId);
    }

    @Override
    public void processEvent(Event event) {
        if (event instanceof ExpiredUnitRemovalEvent) {
            triggerExpiredUnitRemoval(oldestAllowedRecordTime);
        } else {
            Object resource = event.getResource();
            if (resource instanceof String) {
                String processorName = (String) resource;
                if (timeBasedProcessors.containsKey(processorName)) {
                    TimeBasedProcessor<RecordT, UpdateT> processor = timeBasedProcessors.get(processorName);
                    processor.triggerEvent(event.getTime());
                    scheduleEventForTimeBasedProcessor(processor);
                } else {
                    logger.debug("No Processor named {} initialized", processorName);
                }
            }

        }
    }

    private void scheduleEventForTimeBasedProcessor(TimeBasedProcessor<RecordT, UpdateT> timeBasedProcessor) {
        eventManager.newEvent(timeBasedProcessor.getAndIncrementNextTriggerTime(), this)
                .withResource(timeBasedProcessor.getIdentifier()).withNice(0).schedule();
    }

    /**
     * This method acts as a garbage collection for units that didn't send any records since the specified time.
     *
     * @param oldestAllowedRecordTime if all records of a unit are older than this, we assume the unit is no longer sending
     */
    private void triggerExpiredUnitRemoval(Long oldestAllowedRecordTime) {
        List<String> unitsToRemove = new ArrayList<>();
        for (Map.Entry<String, SortedMap<Long, RecordT>> entry : recordBuffer.entrySet()) {
            if (entry.getValue().lastKey() < oldestAllowedRecordTime) {
                unitsToRemove.add(entry.getKey());
            }
        }
        logger.debug("Removing units due to inactivity: {}", unitsToRemove);
        unitsToRemove.forEach(unit -> {
            recordBuffer.remove(unit);
            connectionsBuffer.remove(unit);
        });
    }

    private void scheduleUnitRemoval() {
        lastRemovalTime += config.unitRemovalInterval;
        oldestAllowedRecordTime += config.unitExpirationTime;
        ExpiredUnitRemovalEvent expiredUnitRemovalEvent = new ExpiredUnitRemovalEvent(lastRemovalTime, this);
        eventManager.addEvent(expiredUnitRemovalEvent);
    }

    /**
     * Handles the triggering of the {@link #messageBasedProcessors} if the {@link V2xMessage} is of the correct type.
     *
     * @param receivedV2xMessage message forwarded by the {@link FxdReceiverApp}
     * @param responseRouting    {@link MessageRouting} pointing back to the original sender (Required for building the response)
     */
    public List<V2xMessage> handleMessageAdvanced(ReceivedV2xMessage receivedV2xMessage, MessageRouting responseRouting) {
        List<V2xMessage> responses = new ArrayList<>();
        List<MessageBasedProcessor> handlingProcessors = messageBasedProcessors.stream()
                .filter(processor -> processor.isInstanceOfMessage(receivedV2xMessage))
                .collect(Collectors.toList());
        if (handlingProcessors.isEmpty()) { // check if message can be handled by any processors
            logger.debug("No Processor found to handle message of type {}", receivedV2xMessage.getMessage().getClass());
        }
        handlingProcessors.forEach(processor -> {
            V2xMessage response = processor.handleReceivedMessage(receivedV2xMessage, responseRouting);
            if (response != null) {
                responses.add(response);
            }
        });
        return responses;
    }

    /**
     * This method triggers the {@code shutdown} methods of all configured {@link FxdProcessor FxdProcessors} and cleans up any buffers.
     *
     * @param shutdownTime time of kernel shutdown
     */
    public void shutdown(long shutdownTime) {
        for (FxdProcessor processor : allProcessors) {
            processor.shutdown(shutdownTime);
        }
        recordBuffer.clear();
        connectionsBuffer.clear();
    }

    /**
     * This method is used for specialized handling of {@link UpdateT updates} that is outside the scope of the {@link FxdKernel}
     * implementation.
     *
     * @param update an extension of {@link FxdUpdateMessage} received from a unit
     */
    protected abstract void additionalProcessingOfUpdate(UpdateT update);

    protected abstract TraversalT createTraversal(
            List<RecordT> traversalRecords,
            @Nullable RecordT previousRecord,
            @Nullable RecordT followingRecord
    );
}
