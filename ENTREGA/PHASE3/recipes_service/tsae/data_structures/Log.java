/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import recipes_service.data.Operation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 *         December 2012
 *
 */
public class Log implements Serializable {
    // Only for the zip file with the correct solution of phase1.Needed for the
    // logging system for the phase1. sgeag_2018p
    // private transient LSimCoordinator lsim =
    // LSimFactory.getCoordinatorInstance();
    // Needed for the logging system sgeag@2017
    // private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

    private static final long serialVersionUID = -4864990265268259700L;
    /**
     * This class implements a log, that stores the operations
     * received by a client.
     * They are stored in a ConcurrentHashMap (a hash table),
     * that stores a list of operations for each member of
     * the group.
     */
    // private ConcurrentHashMap<String, List<Operation>> log= new
    // ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Operation>> log = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Log(List<String> participants) {
        for (String participant : participants) {
            log.put(participant, new CopyOnWriteArrayList<>());
        }
    }

    /**
     * Inserts an operation into the log. Operations are
     * inserted in order. If the last operation for
     * the user is not the previous operation than the one
     * being inserted, the insertion will fail.
     *
     * @param op the operation to be inserted into the log.
     * @return true if op is inserted, false otherwise.
     */
    public boolean add(Operation op) {
        // Retrieve the host ID from the operation's timestamp
        String hostId = op.getTimestamp().getHostid();
        // Get or create the list of operations for the host
        CopyOnWriteArrayList<Operation> opeList = log.computeIfAbsent(hostId, k -> new CopyOnWriteArrayList<>());

        // Check for duplicate operations based on timestamp
        if (opeList.stream().anyMatch(existingOp -> existingOp.getTimestamp().equals(op.getTimestamp()))) {
            LSimLogger.log(Level.WARN, "Duplicate operation detected: " + op);
            return false; // Duplicate timestamp, ignore the operation
        }

        // Ensure operations are added in order based on timestamp
        if (opeList.isEmpty() || opeList.get(opeList.size() - 1).getTimestamp().compare(op.getTimestamp()) < 0) {
            opeList.add(op);
            LSimLogger.log(Level.INFO, String.format("Operation added: Host='%s', Timestamp='%s'. Current size: %d",
                    hostId, op.getTimestamp(), opeList.size()));
            return true;
        }

        // Log a warning if the operation is out of order
        LSimLogger.log(Level.WARN, String.format("Operation rejected due to out-of-order timestamp: Host='%s', Timestamp='%s'",
                hostId, op.getTimestamp()));
        return false;
    }

    /**
     * Checks the received summary (sum) and determines the operations
     * contained in the log that have not been seen by
     * the proprietary of the summary.
     * Returns them in an ordered list.
     * 
     * @param sum the summary vector to compare against.
     * @return list of operations that are newer than the summary.
     */
    public List<Operation> listNewer(TimestampVector sum) {
        List<Operation> newList = new ArrayList<>();
        lock.readLock().lock(); // Acquire read lock for thread safety
        try {
            // Iterate over each host's operation list in the log
            for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
                String id = entry.getKey();
                CopyOnWriteArrayList<Operation> opeList = entry.getValue();
                if (opeList.isEmpty()) continue; // Skip empty lists

                // Get the last seen timestamp for the host
                Timestamp lastSeen = sum.getLast(id);
                // Add operations that are newer than the last seen timestamp
                for (Operation op : opeList) {
                    if (op.getTimestamp().compare(lastSeen) > 0) {
                        newList.add(op);
                        LSimLogger.log(Level.TRACE, "Operation newer than summary found: " + op);
                    }
                }
            }
        } finally {
            lock.readLock().unlock(); // Release read lock
        }
        return newList;
    }

    /**
     * Removes from the log the operations that have
     * been acknowledged by all the members
     * of the group, according to the provided
     * ackSummary.
     * 
     * @param ack the acknowledgment matrix.
     */
    public void purgeLog(TimestampMatrix ack) {
        if (ack == null) return; // Return if ack is null

        lock.writeLock().lock(); // Acquire write lock for thread safety
        try {
            // Get minimum timestamp vector from ack matrix
            TimestampVector minTimestampVector = ack.minTimestampVector();
            if (minTimestampVector == null) return; // Return if minTimestampVector is null

            // Iterate over each host's operation list in the log
            for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
                String hostId = entry.getKey();
                CopyOnWriteArrayList<Operation> operations = entry.getValue();

                // Remove operations that have been acknowledged by all participants
                operations.removeIf(op -> {
                    String opHostId = op.getTimestamp().getHostid();
                    Timestamp minAck = minTimestampVector.getLast(opHostId);
                    return minAck != null && op.getTimestamp().compare(minAck) <= 0;
                });
            }
        } finally {
            lock.writeLock().unlock(); // Release write lock
        }
    }

    /**
     * Checks if this log is equal to another object.
     * 
     * @param obj the object to compare with.
     * @return true if the logs are equal, false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true; // Return true if the same object
        if (obj == null || getClass() != obj.getClass())
            return false; // Return false if obj is null or not the same class

        lock.readLock().lock(); // Acquire read lock for thread safety
        try {
            Log other = (Log) obj;
            return log.equals(other.log); // Compare the log maps
        } finally {
            lock.readLock().unlock(); // Release read lock
        }
    }

    /**
     * Checks if the log contains an operation with the given timestamp.
     *
     * @param timestamp the timestamp to check for.
     * @return true if the log contains an operation with the given timestamp, false otherwise.
     */
    public boolean contains(Timestamp timestamp) {
        if (timestamp == null) {
            return false; // Return false if timestamp is null
        }

        // Iterate over each host's operation list in the log
        for (CopyOnWriteArrayList<Operation> operations : log.values()) {
            // Check if any operation matches the given timestamp
            for (Operation operation : operations) {
                if (operation.getTimestamp().equals(timestamp)) {
                    return true; // Return true if a match is found
                }
            }
        }
        return false; // Return false if no match is found
    }

    /**
     * Retrieves the timestamp of a given operation.
     * Assumes the operation exists in the log.
     *
     * @param op the operation to retrieve the timestamp for.
     * @return the timestamp of the operation, or null if not found.
     */
    public Timestamp getTimestampForOperation(Operation op) {
        // Iterate over each host's operation list in the log
        for (CopyOnWriteArrayList<Operation> operations : log.values()) {
            // Check if the operation exists in the list
            for (Operation operation : operations) {
                if (operation.equals(op)) {
                    return operation.getTimestamp(); // Return the timestamp if found
                }
            }
        }
        return null; // Return null if the operation is not found
    }


    /**
     * toString
     */
    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (CopyOnWriteArrayList<Operation> sublog : log.values()) {
                for (Operation op : sublog) {
                    sb.append(op.toString()).append("\n");
                }
            }
            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}