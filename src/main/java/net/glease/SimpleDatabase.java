package net.glease;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.IntStream;

public abstract class SimpleDatabase<T> {
    public static final double BULK_LOOKUP_FACTOR = 0.1;
    public static final int BULK_LOOKUP_THRESHOLD_LOWER = 10;
    public static final int BULK_LOOKUP_THRESHOLD_UPPER = 100;
    private final TreeMap<Integer, T> mapDB = new TreeMap<>();
    DBEntry<T>[] cache = null;
    int off;

    private DBEntry<T> createEntry(int id) {
        T v = mapDB.get(id);
        return v == null ? null : new DBEntry<>(id, v);
    }

    @SuppressWarnings("unchecked")
    void bake() {
        off = mapDB.firstKey();
        cache = IntStream.range(off, mapDB.lastKey() + 1).mapToObj(this::createEntry).toArray(DBEntry[]::new);
    }

    private final BitSet idMap = new BitSet();
    private List<DBEntry<T>> refCache = null;

    public synchronized int nextID() {
        return idMap.nextClearBit(0);
    }

    public synchronized DBEntry<T> add(int id, T value) {
        if (value == null) {
            throw new NullPointerException("Value cannot be null");
        } else if (id < 0) {
            throw new IllegalArgumentException("ID cannot be negative");
        } else {
            if (mapDB.putIfAbsent(id, value) == null) {
                idMap.set(id);
                refCache = null;
                return new DBEntry<>(id, value);
            } else {
                throw new IllegalArgumentException("ID or value is already contained within database");
            }
        }
    }

    public synchronized boolean removeID(int key) {
        if (key < 0) return false;

        if (mapDB.remove(key) != null) {
            idMap.clear(key);
            refCache = null;
            return true;
        }

        return false;
    }

    public synchronized boolean removeValue(T value) {
        return value != null && removeID(getID(value));
    }

    public synchronized int getID(T value) {
        if (value == null) return -1;

        for (DBEntry<T> entry : getEntries()) {
            if (entry.getValue() == value) return entry.getID();
        }

        return -1;
    }

    public synchronized T getValue(int id) {
        if (id < 0 || mapDB.size() <= 0) return null;
        return mapDB.get(id);
    }

    public synchronized int size() {
        return mapDB.size();
    }

    public synchronized void reset() {
        mapDB.clear();
        idMap.clear();
        refCache = Collections.emptyList();
    }

    public synchronized List<DBEntry<T>> getEntries() {
        if (refCache == null) {
            List<DBEntry<T>> temp = new ArrayList<>();
            for (Entry<Integer, T> entry : mapDB.entrySet()) {
                temp.add(new DBEntry<>(entry.getKey(), entry.getValue()));
            }
            refCache = Collections.unmodifiableList(temp);
        }

        return refCache;
    }

    /**
     * If argument size is big then a sort-merge join will be performed.
     * Otherwise it is equivalent to looking up individual elements separately via {@link #getValue(int)}.
     */
    public synchronized List<DBEntry<T>> bulkLookup(int... keys) {
        if (keys.length <= 0) return Collections.emptyList();

        int[] sortedKeys = new int[keys.length];
        System.arraycopy(keys, 0, sortedKeys, 0, keys.length);
        Arrays.sort(sortedKeys);

        List<DBEntry<T>> subList = new ArrayList<>(keys.length);
        int n = 0;

        for (DBEntry<T> entry : getEntries()) {
            while (n < sortedKeys.length && sortedKeys[n] < entry.getID()) n++;
            if (n >= sortedKeys.length) break;
            if (sortedKeys[n] == entry.getID()) subList.add(entry);
        }

        return subList;
    }

    public synchronized List<DBEntry<T>> bulkLookupNaive(int... keys) {
        if (keys.length <= 0) return Collections.emptyList();
        List<DBEntry<T>> list = new ArrayList<>(keys.length);
        for (int k : keys) {
            final T element = mapDB.get(k);
            if (element != null) {
                // it shouldn't place too much allocation/gc pressure since there aren't too many keys to look up anyway
                list.add(new DBEntry<>(k, element));
            }
        }
        return list;
    }

    public synchronized List<DBEntry<T>> bulkLookupBaked(int... keys) {
        if (keys.length <= 0) return Collections.emptyList();
        List<DBEntry<T>> list = new ArrayList<>(keys.length);
        for (int k : keys) {
            final DBEntry<T> element = cache[k - off];
            if (element != null) {
                // it shouldn't place too much allocation/gc pressure since there aren't too many keys to look up anyway
                list.add(element);
            }
        }
        return list;
    }
}