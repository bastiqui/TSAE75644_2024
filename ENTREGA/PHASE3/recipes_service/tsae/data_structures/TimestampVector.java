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
     */
    public void updateTimestamp(Timestamp timestamp) {
        if (timestamp == null) {
            LSimLogger.log(Level.WARN, "Attempted to update TimestampVector with a null timestamp.");
            return;
        }

        timestampVector.compute(timestamp.getHostid(), (id, currentTS) -> {
            if (currentTS == null || timestamp.compare(currentTS) > 0) {
                return timestamp;
            }
            return currentTS;
        });
    }

    /**
     * Returns the ConcurrentHashMap containing all timestamps
     */
    public ConcurrentHashMap<String, Timestamp> getTimestamps() {
        return timestampVector;
    }

    /**
     * Updates the timestamp for a specific participant
     */
    public void update(String participant, Timestamp timestamp) {
        if (participant == null || timestamp == null) {
            return;
        }
        timestampVector.put(participant, timestamp);
    }

    /**
     * Merges the received vector, keeping the maximum for each hostId.
     */
    public void updateMax(TimestampVector tsVector) {
        if (tsVector == null) {
            LSimLogger.log(Level.WARN, "Attempted to merge with a null TimestampVector.");
            return;
        }

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
     */
    public Timestamp getLast(String node) {
        return timestampVector.get(node);
    }

    /**
     * Merges the received vector, keeping the minimum for each hostId.
     */
    public void mergeMin(TimestampVector tsVector) {
        if (tsVector == null) {
            LSimLogger.log(Level.WARN, "Attempted to mergeMin with a null TimestampVector.");
            return;
        }

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

    public void updateMaxWithTolerance(TimestampVector tsVector) {
        if (tsVector == null) {
            LSimLogger.log(Level.WARN, "Attempted to updateMaxWithTolerance with a null TimestampVector.");
            return;
        }

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
     */
    @Override
    public TimestampVector clone() {
        lock.readLock().lock();
        try {
            TimestampVector cloned = new TimestampVector(
                    Arrays.asList(timestampVector.keySet().toArray(new String[0])));
            timestampVector.forEach(cloned.timestampVector::put);
            return cloned;
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * Checks equality between two TimestampVectors.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;

        lock.readLock().lock();
        try {
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