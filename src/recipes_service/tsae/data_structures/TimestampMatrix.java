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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable {

    private static final long serialVersionUID = 3331148113387926667L;
    private ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<>();

    public TimestampMatrix(List<String> participants) {
        // Create an empty TimestampMatrix
        for (String participant : participants) {
            timestampMatrix.put(participant, new TimestampVector(participants));
        }
    }

    /**
     * @param node
     * @return the timestamp vector of node in this timestamp matrix
     */
    public TimestampVector getTimestampVector(String node) {
        return timestampMatrix.get(node);
    }

    /**
     * Merges two timestamp matrices taking the elementwise maximum
     * @param tsMatrix
     */
    public synchronized void updateMax(TimestampMatrix tsMatrix) {
        for (String node : tsMatrix.timestampMatrix.keySet()) {
            timestampMatrix.merge(node, tsMatrix.getTimestampVector(node), (current, other) -> {
                current.updateMax(other);
                return current;
            });
        }
    }

    /**
     * Substitutes current timestamp vector of node for tsVector
     * @param node
     * @param tsVector
     */
    public void update(String node, TimestampVector tsVector) {
        timestampMatrix.put(node, tsVector);
    }

    public synchronized TimestampVector minTimestampVector() {
        TimestampVector minVector = new TimestampVector(new ArrayList<>(timestampMatrix.keySet()));
        for (TimestampVector vector : timestampMatrix.values()) {
            minVector.mergeMin(vector);
        }
        return minVector;
    }

    /**
     * Clone
     */
    @Override
    public synchronized TimestampMatrix clone() {
        TimestampMatrix clonedMatrix = new TimestampMatrix(new ArrayList<>(timestampMatrix.keySet()));
        for (String node : timestampMatrix.keySet()) {
            clonedMatrix.timestampMatrix.put(node, timestampMatrix.get(node).clone());
        }
        return clonedMatrix;
    }

    /**
     * Equals
     */
    @Override
    public synchronized boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TimestampMatrix otherMatrix = (TimestampMatrix) obj;
        return timestampMatrix.equals(otherMatrix.timestampMatrix);
    }

    /**
     * toString
     */
    @Override
    public synchronized String toString() {
        StringBuilder all = new StringBuilder();
        if (timestampMatrix == null) {
            return all.toString();
        }
        for (Enumeration<String> en = timestampMatrix.keys(); en.hasMoreElements(); ) {
            String name = en.nextElement();
            if (timestampMatrix.get(name) != null) {
                all.append(name).append(":   ").append(timestampMatrix.get(name)).append("\n");
            }
        }
        return all.toString();
    }
}