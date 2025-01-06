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
     * @param op the operation to be inserted into the log
     * @return true if the operation is inserted, false otherwise
     */
    public boolean add(Operation op) {
        // Retrieve the host ID from the operation's timestamp
        String hostId = op.getTimestamp().getHostid();
        
        // Get the list of operations for the host ID, or create a new list if it doesn't exist
        CopyOnWriteArrayList<Operation> opeList = log.computeIfAbsent(hostId, k -> new CopyOnWriteArrayList<>());

        // Check if the list is empty or if the last operation's timestamp is less than the new operation's timestamp
        if (opeList.isEmpty() || opeList.get(opeList.size() - 1).getTimestamp().compare(op.getTimestamp()) < 0) {
            // Add the operation to the list
            opeList.add(op);
            return true;
        }
        // Return false if the operation cannot be inserted in order
        return false;
    }

    /**
     * Checks the received summary (sum) and determines the operations
     * contained in the log that have not been seen by
     * the proprietary of the summary.
     * Returns them in an ordered list.
     * 
     * @param sum the summary vector to compare against
     * @return list of operations that are newer than the summary
     */
    public List<Operation> listNewer(TimestampVector sum) {
        // Create a new list to store operations that are newer than the summary
        List<Operation> newList = new ArrayList<>();
        
        // Acquire a read lock to ensure thread safety
        lock.readLock().lock();
        try {
            // Iterate over each entry in the log
            for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
                String id = entry.getKey();
                CopyOnWriteArrayList<Operation> opeList = entry.getValue();
                
                // Skip empty operation lists
                if (opeList.isEmpty())
                    continue;

                // Get the last seen timestamp for the current host ID
                Timestamp lastSeen = sum.getLast(id);
                
                // Add operations that are newer than the last seen timestamp to the new list
                for (Operation op : opeList) {
                    if (op.getTimestamp().compare(lastSeen) > 0) {
                        newList.add(op);
                    }
                }
            }
        } finally {
            // Release the read lock
            lock.readLock().unlock();
        }
        // Return the list of newer operations
        return newList;
    }

    /**
     * Removes from the log the operations that have
     * been acknowledged by all the members
     * of the group, according to the provided
     * ackSummary.
     * 
     * @param ack the acknowledgment matrix used to determine which operations to remove
     */
    public void purgeLog(TimestampMatrix ack) {
        // Acquire a write lock to ensure thread safety during modification
        lock.writeLock().lock();
        try {
            // Iterate over each entry in the log
            for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
                String id = entry.getKey();
                CopyOnWriteArrayList<Operation> opeList = entry.getValue();

                // Get the acknowledgment vector for the current host ID
                TimestampVector ackVector = ack.getTimestampVector(id);
                
                // Remove operations that have been acknowledged by all participants
                opeList.removeIf(
                        op -> op.getTimestamp().compare(ackVector.getLast(op.getTimestamp().getHostid())) <= 0);
            }
        } finally {
            // Release the write lock
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if this log is equal to another object.
     * 
     * @param obj the object to compare with
     * @return true if the logs are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        // Check if the objects are the same instance
        if (this == obj)
            return true;
        
        // Check if the object is null or of a different class
        if (obj == null || getClass() != obj.getClass())
            return false;

        // Acquire a read lock to ensure thread safety
        lock.readLock().lock();
        try {
            // Cast the object to a Log and compare the internal log structures
            Log other = (Log) obj;
            return log.equals(other.log);
        } finally {
            // Release the read lock
            lock.readLock().unlock();
        }
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