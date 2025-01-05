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

    public TimestampVector(List<String> participants){
        // Inicializamos con null timestamps
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
     * Actualiza (si es más reciente) el timestamp de 'timestampVector' con el timestamp proporcionado.
     */
    public void updateTimestamp(Timestamp timestamp){
        if (timestamp == null) {
            LSimLogger.log(Level.WARN, "Attempted to update TimestampVector with a null timestamp.");
            return;
        }
        lock.writeLock().lock();
        try {
            String id = timestamp.getHostid();
            Timestamp currentTS = timestampVector.get(id);
            if (currentTS == null || timestamp.compare(currentTS) > 0) {
                timestampVector.put(id, timestamp);
                LSimLogger.log(Level.TRACE, "Updated timestamp for " + id + " to " + timestamp);
            } else {
                LSimLogger.log(Level.TRACE, "Skipped update for " + id
                               + ". Current timestamp: " + currentTS
                               + ", New timestamp: " + timestamp);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Fusiona el 'tsVector' recibido tomando el máximo para cada 'hostId'.
     */
    public void updateMax(TimestampVector tsVector) {
        if (tsVector == null) return;
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, Timestamp> entry : tsVector.timestampVector.entrySet()) {
                String id = entry.getKey();
                Timestamp incomingTS = entry.getValue();
                Timestamp localTS = this.timestampVector.get(id);
                if (incomingTS != null && (localTS == null || incomingTS.compare(localTS) > 0)) {
                    this.timestampVector.put(id, incomingTS);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Devuelve el último timestamp conocido para el nodo 'node'.
     */
    public Timestamp getLast(String node){
        lock.readLock().lock();
        try {
            return timestampVector.get(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Fusiona el 'tsVector' recibido tomando el mínimo para cada 'hostId'.
     */
    public void mergeMin(TimestampVector tsVector){
        if (tsVector == null) return;
        lock.writeLock().lock();
        try {
            for (String id : tsVector.timestampVector.keySet()) {
                Timestamp incomingTS = tsVector.timestampVector.get(id);
                Timestamp localTS = this.timestampVector.get(id);
                if (incomingTS != null && (localTS == null || incomingTS.compare(localTS) < 0)) {
                    this.timestampVector.put(id, incomingTS);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Devuelve una copia de este 'TimestampVector'.
     */
    @Override
    public TimestampVector clone(){
        lock.readLock().lock();
        try {
            TimestampVector cloned = new TimestampVector(Arrays.asList(timestampVector.keySet().toArray(new String[0])));
            for (String id : timestampVector.keySet()) {
                cloned.timestampVector.put(id, timestampVector.get(id));
            }
            return cloned;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * equals
     */
    @Override
    public boolean equals(Object obj){
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TimestampVector comp = (TimestampVector) obj;

        // Para comparar con seguridad, leemos ambas estructuras bajo el candado de lectura
        lock.readLock().lock();
        try {
            return timestampVector.equals(comp.timestampVector);
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
            for (String key : timestampVector.keySet()) {
                Timestamp ts = timestampVector.get(key);
                sb.append(ts != null ? ts.toString() : "null").append("\n");
            }
            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}