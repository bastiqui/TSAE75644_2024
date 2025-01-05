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
     * inserts an operation into the log. Operations are
     * inserted in order. If the last operation for
     * the user is not the previous operation than the one
     * being inserted, the insertion will fail.
     *
     * @param op
     * @return true if op is inserted, false otherwise.
     */
    public boolean add(Operation op) {
        String hostId = op.getTimestamp().getHostid();
        CopyOnWriteArrayList<Operation> opeList = log.computeIfAbsent(hostId, k -> new CopyOnWriteArrayList<>());

        if (opeList.isEmpty() || opeList.get(opeList.size() - 1).getTimestamp().compare(op.getTimestamp()) < 0) {
            opeList.add(op);
            return true;
        }
        return false;
    }

    /**
     * Checks the received summary (sum) and determines the operations
     * contained in the log that have not been seen by
     * the proprietary of the summary.
     * Returns them in an ordered list.
     * 
     * @param sum
     * @return list of operations
     */
    public List<Operation> listNewer(TimestampVector sum) {
        List<Operation> newList = new ArrayList<>();
        lock.readLock().lock();
        try {
            for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
                String id = entry.getKey();
                CopyOnWriteArrayList<Operation> opeList = entry.getValue();
                if (opeList.isEmpty())
                    continue;

                Timestamp lastSeen = sum.getLast(id);
                for (Operation op : opeList) {
                    if (op.getTimestamp().compare(lastSeen) > 0) {
                        newList.add(op);
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return newList;
    }

    /**
     * Removes from the log the operations that have
     * been acknowledged by all the members
     * of the group, according to the provided
     * ackSummary.
     * 
     * @param ack: ackSummary.
     */
    public void purgeLog(TimestampMatrix ack) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, CopyOnWriteArrayList<Operation>> entry : log.entrySet()) {
                String id = entry.getKey();
                CopyOnWriteArrayList<Operation> opeList = entry.getValue();

                TimestampVector ackVector = ack.getTimestampVector(id);
                opeList.removeIf(
                        op -> op.getTimestamp().compare(ackVector.getLast(op.getTimestamp().getHostid())) <= 0);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * equals
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;

        lock.readLock().lock();
        try {
            Log other = (Log) obj;
            return log.equals(other.log);
        } finally {
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