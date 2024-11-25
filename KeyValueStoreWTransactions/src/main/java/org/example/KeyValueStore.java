package org.example;

import java.util.*;

public class KeyValueStore {

    private final Map<String, String> permDataStore = new HashMap<>();
    private final Map<Long, Map<String, String>> threadTempDataStore = new HashMap<>();
    private static final String TOMBSTONE = "SPECIAL_VALUE_TOMBSTONE";

    // Transaction is between begin and commit
    private boolean isInGroup() {
        long currentThreadId = Thread.currentThread().threadId();
        return threadTempDataStore.containsKey(currentThreadId);
    }

    private Map<String, String> getTempDataStore() {
        long currentThreadId = Thread.currentThread().threadId();
        return threadTempDataStore.get(currentThreadId);
    }

    public void set(String key, String value) {
        if (isInGroup()) {
            Map<String, String> tempDataStore = getTempDataStore();
            tempDataStore.put(key, value);
        } else {
            permDataStore.put(key, value);
        }
    }

    public String get(String key) {
        if (isInGroup()) {
            Map<String, String> tempDataStore = getTempDataStore();
            if (!tempDataStore.containsKey(key)) {
                return permDataStore.get(key);
            }
            String val = tempDataStore.get(key);
            if (val == null || val.equals(TOMBSTONE)) {
                return null;
            }
            return val;
        }
        return permDataStore.get(key);
    }

    public void delete(String key) {
        if (isInGroup()){
            Map<String, String> tempDataStore = getTempDataStore();
            if (permDataStore.containsKey(key)) {
                tempDataStore.put(key, TOMBSTONE);
            } else {
                tempDataStore.remove(key);
            }
        } else {
            permDataStore.remove(key);
        }
    }

    public void begin() {
        long currentThreadId = Thread.currentThread().threadId();
        threadTempDataStore.put(currentThreadId, new HashMap<String, String>());
    }

    public void commit() {
        Map<String, String> tempDataStore = getTempDataStore();
        for (Map.Entry<String, String> entry : tempDataStore.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            if (val.equals(TOMBSTONE)) {
                permDataStore.remove(key);
            } else {
                permDataStore.put(key, val);
            }
        }
        clearThreadEntries();
    }

    public void rollback() {
        clearThreadEntries();
    }

    private void clearThreadEntries() {
        long currentThreadId = Thread.currentThread().threadId();
        threadTempDataStore.remove(currentThreadId);
    }

}
