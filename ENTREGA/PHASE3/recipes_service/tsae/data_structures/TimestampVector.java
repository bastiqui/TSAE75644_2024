package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

public class TimestampVector implements Serializable {
    private static final long serialVersionUID = -765026247959198886L;

    private final ConcurrentHashMap<String, Timestamp> timestampVector = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public TimestampVector(List<String> participants) {
        lock.writeLock().lock();
        try {
            for (String id : participants) {
                timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates the timestamp in the vector if it is more recent.
     * 
     * @param timestamp the new timestamp to potentially update the vector with
     */
    public void updateTimestamp(Timestamp timestamp) {
        if (timestamp == null) {
            // Log a warning if the provided timestamp is null
            LSimLogger.log(Level.WARN, "Attempted to update TimestampVector with a null timestamp.");
            return;
        }

        // Update the timestamp for the host ID if the new timestamp is more recent
        timestampVector.compute(timestamp.getHostid(), (id, currentTS) -> {
            if (currentTS == null || timestamp.compare(currentTS) > 0) {
                return timestamp;
            }
            return currentTS;
        });
    }

    /**
     * Returns the ConcurrentHashMap containing all timestamps.
     * 
     * @return the map of host IDs to their respective timestamps
     */
    public ConcurrentHashMap<String, Timestamp> getTimestamps() {
        return timestampVector;
    }

    /**
     * Updates the timestamp for a specific participant.
     * 
     * @param participant the ID of the participant
     * @param timestamp the new timestamp to set for the participant
     */
    public void update(String participant, Timestamp timestamp) {
        if (participant == null || timestamp == null) {
            // Do nothing if either the participant or timestamp is null
            return;
        }
        // Update the timestamp for the given participant
        timestampVector.put(participant, timestamp);
    }

    /**
     * Merges the received vector, keeping the maximum for each hostId.
     * 
     * @param tsVector the incoming TimestampVector to merge with
     */
    public void updateMax(TimestampVector tsVector) {
        if (tsVector == null) {
            // Log a warning if the provided TimestampVector is null
            LSimLogger.log(Level.WARN, "Attempted to merge with a null TimestampVector.");
            return;
        }

        // For each entry in the incoming vector, update the local vector with the maximum timestamp
        tsVector.timestampVector.forEach((id, incomingTS) -> timestampVector.compute(id,
                (key, localTS) -> {
                    if (incomingTS != null && (localTS == null || incomingTS.compare(localTS) > 0)) {
                        return incomingTS;
                    }
                    return localTS;
                }));
    }

    /**
     * Returns the last known timestamp for the given node.
     * 
     * @param node the ID of the node
     * @return the last known timestamp for the node
     */
    public Timestamp getLast(String node) {
        return timestampVector.get(node);
    }

    /**
     * Merges the received vector, keeping the minimum for each hostId.
     * 
     * @param tsVector the incoming TimestampVector to merge with
     */
    public void mergeMin(TimestampVector tsVector) {
        if (tsVector == null) {
            // Log a warning if the provided TimestampVector is null
            LSimLogger.log(Level.WARN, "Attempted to mergeMin with a null TimestampVector.");
            return;
        }

        // For each entry in the incoming vector, update the local vector with the minimum timestamp
        tsVector.timestampVector.forEach((id, incomingTS) -> timestampVector.compute(id,
                (key, localTS) -> {
                    if (incomingTS == null) {
                        // If the incoming timestamp is null, keep the local one
                        return localTS;
                    }
                    if (localTS == null || incomingTS.compare(localTS) < 0) {
                        // If the local timestamp is null or greater, use the incoming one
                        return incomingTS;
                    }
                    return localTS;
                }));
    }

    /**
     * Updates the vector with the maximum timestamp for each hostId, with tolerance.
     * 
     * @param tsVector the incoming TimestampVector to merge with
     */
    public void updateMaxWithTolerance(TimestampVector tsVector) {
        if (tsVector == null) {
            // Log a warning if the provided TimestampVector is null
            LSimLogger.log(Level.WARN, "Attempted to updateMaxWithTolerance with a null TimestampVector.");
            return;
        }

        // For each entry in the incoming vector, update the local vector with the maximum timestamp
        tsVector.timestampVector.forEach((id, incomingTS) -> timestampVector.compute(id,
                (key, localTS) -> {
                    if (incomingTS == null) {
                        // If the incoming timestamp is null, keep the local one
                        return localTS;
                    }
                    if (localTS == null || incomingTS.compare(localTS) > 0) {
                        // If the local timestamp is null or smaller, use the incoming one
                        return incomingTS;
                    }
                    return localTS;
                }));
    }

    /**
     * Returns a clone of this TimestampVector.
     * 
     * @return a new TimestampVector that is a copy of this one
     */
    @Override
    public TimestampVector clone() {
        lock.readLock().lock();
        try {
            // Create a new TimestampVector with the same keys
            TimestampVector cloned = new TimestampVector(
                    Arrays.asList(timestampVector.keySet().toArray(new String[0])));
            // Copy each timestamp to the new vector
            timestampVector.forEach(cloned.timestampVector::put);
            return cloned;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks equality between two TimestampVectors.
     * 
     * @param obj the object to compare with
     * @return true if the vectors are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;

        lock.readLock().lock();
        try {
            // Compare the internal maps for equality
            TimestampVector comp = (TimestampVector) obj;
            return timestampVector.equals(comp.timestampVector);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Converts the TimestampVector to a string.
     */
    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            timestampVector.forEach((key, ts) -> sb.append(ts != null ? ts.toString() : "null").append("\n"));
            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}