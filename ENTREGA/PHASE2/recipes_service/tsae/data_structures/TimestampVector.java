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
     * @param timestamp the new timestamp to potentially update in the vector
     */
    public void updateTimestamp(Timestamp timestamp) {
        // Check if the provided timestamp is null and log a warning if so
        if (timestamp == null) {
            LSimLogger.log(Level.WARN, "Attempted to update TimestampVector with a null timestamp.");
            return;
        }
        // Update the timestamp for the host ID if the new timestamp is more recent
        timestampVector.compute(timestamp.getHostid(), (id, currentTS) -> {
            // If the current timestamp is null or the new timestamp is more recent, update it
            if (currentTS == null || timestamp.compare(currentTS) > 0) {
                LSimLogger.log(Level.TRACE, "Updated timestamp for " + id + " to " + timestamp);
                return timestamp;
            }
            // Log that the update was skipped because the current timestamp is more recent
            LSimLogger.log(Level.TRACE, "Skipped update for " + id
                    + ". Current timestamp: " + currentTS
                    + ", New timestamp: " + timestamp);
            return currentTS;
        });
    }

    /**
     * Merges the received vector, keeping the maximum for each hostId.
     * 
     * @param tsVector the incoming timestamp vector to merge
     */
    public void updateMax(TimestampVector tsVector) {
        // Return immediately if the incoming vector is null
        if (tsVector == null)
            return;

        // Iterate over each entry in the incoming vector
        tsVector.timestampVector.forEach((id, incomingTS) -> 
            // Update the local timestamp to the maximum of the local and incoming timestamps
            timestampVector.compute(id, (key, localTS) -> 
                (incomingTS != null && (localTS == null || incomingTS.compare(localTS) > 0))
                        ? incomingTS
                        : localTS));
    }

    /**
     * Returns the last known timestamp for the given node.
     * 
     * @param node the identifier of the node
     * @return the last known timestamp for the specified node
     */
    public Timestamp getLast(String node) {
        // Retrieve the last known timestamp for the specified node
        return timestampVector.get(node);
    }

    /**
     * Merges the received vector, keeping the minimum for each hostId.
     * 
     * @param tsVector the incoming timestamp vector to merge
     */
    public void mergeMin(TimestampVector tsVector) {
        // Return immediately if the incoming vector is null
        if (tsVector == null)
            return;

        // Iterate over each entry in the incoming vector
        tsVector.timestampVector.forEach((id, incomingTS) -> 
            // Update the local timestamp to the minimum of the local and incoming timestamps
            timestampVector.compute(id, (key, localTS) -> 
                (incomingTS != null && (localTS == null || incomingTS.compare(localTS) < 0))
                        ? incomingTS
                        : localTS));
    }

    /**
     * Returns a clone of this TimestampVector.
     * 
     * @return a new TimestampVector that is a clone of the current one
     */
    @Override
    public TimestampVector clone() {
        // Acquire a read lock to ensure thread safety during cloning
        lock.readLock().lock();
        try {
            // Create a new TimestampVector with the same keys
            TimestampVector cloned = new TimestampVector(
                    Arrays.asList(timestampVector.keySet().toArray(new String[0])));
            // Copy each timestamp from the current vector to the cloned vector
            timestampVector.forEach(cloned.timestampVector::put);
            return cloned;
        } finally {
            // Release the read lock
            lock.readLock().unlock();
        }
    }

    /**
     * Checks equality between two TimestampVectors.
     * 
     * @param obj the object to compare with
     * @return true if the two TimestampVectors are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        // Check if the objects are the same instance
        if (this == obj)
            return true;
        // Check if the object is null or of a different class
        if (obj == null || getClass() != obj.getClass())
            return false;

        // Acquire a read lock to ensure thread safety during comparison
        lock.readLock().lock();
        try {
            // Cast the object to a TimestampVector and compare the internal maps
            TimestampVector comp = (TimestampVector) obj;
            return timestampVector.equals(comp.timestampVector);
        } finally {
            // Release the read lock
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