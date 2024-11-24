package org.example;

import java.util.*;

// Assume serializable isolation
public class KeyValueStore {
    // Assuming key value store stores strings
    private final Map<String, String> permDataStore = new HashMap<>();
    private final Map<String, String> tempDataStore = new HashMap<>();
    boolean inGroup = false;
    private final Set<String> tombstoneKeys = new HashSet<>();

    public void set(String key, String value) {
        if (!inGroup) {
            permDataStore.put(key, value);
        } else {
            tempDataStore.put(key, value);
        }
    }

    public String get(String key) {
        if (!inGroup) {
            return permDataStore.get(key);
        } else {
            String val = tempDataStore.get(key);
            if (val == null) {
                if (tombstoneKeys.contains(key)) {
                    return null;
                }
                val = permDataStore.get(key);
            }
            return val;
        }
    }

    public void delete(String key) {
        if (!inGroup) {
            permDataStore.remove(key);
        } else {
            if (tempDataStore.containsKey(key)) {
                tempDataStore.remove(key);
            } else {
                tombstoneKeys.add(key);
            }
        }
    }

    public void begin() {
        inGroup = true;
    }

    public void commit() {
        for (Map.Entry<String, String> entry : tempDataStore.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            permDataStore.put(key, value);
        }

        for (String key : tombstoneKeys) {
            permDataStore.remove(key);
        }

        tempDataStore.clear();
        tombstoneKeys.clear();;
        inGroup = false;
    }

    public void rollback() {
        tempDataStore.clear();
        tombstoneKeys.clear();;
        inGroup = false;
    }

}
