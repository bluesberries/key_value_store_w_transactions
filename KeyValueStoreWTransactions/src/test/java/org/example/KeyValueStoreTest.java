package org.example;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueStoreTest {

    @BeforeEach
    void setUp() {

    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void setGet() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "hello";
        String inputVal = "world";
        dataStore.set(key, inputVal);
        String outputVal = dataStore.get(key);
        assertEquals(inputVal, outputVal);
    }

    @Test
    void delete() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "hello";
        String inputVal = "world";
        dataStore.set(key, inputVal);
        dataStore.delete(key);
        String outputVal = dataStore.get(key);
        assertNull(outputVal);
    }

    @Test
    void transaction() {
        KeyValueStore dataStore = new KeyValueStore();
        dataStore.begin();
        String key = "hello";
        String inputVal = "world";
        dataStore.set(key, inputVal);
        assertEquals(inputVal, dataStore.get(key));
        dataStore.commit();
        assertEquals(inputVal, dataStore.get(key));
    }

    @Test
    void transactionRollback() {
        KeyValueStore dataStore = new KeyValueStore();
        dataStore.begin();
        String key = "hello";
        String inputVal = "world";
        dataStore.set(key, inputVal);
        assertEquals(inputVal, dataStore.get(key));
        dataStore.rollback();
        assertNull(dataStore.get(key));
    }

    @Test
    void transactionDeleteCommit() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "hello";
        String inputVal = "world";
        dataStore.set(key, inputVal);

        dataStore.begin();
        dataStore.delete(key);
        dataStore.commit();

        assertNull(dataStore.get(key));
    }

    @Test
    void transactionDeleteRollback() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "hello";
        String inputVal = "world";
        dataStore.set(key, inputVal);

        dataStore.begin();
        dataStore.delete(key);
        dataStore.rollback();

        assertEquals(inputVal, dataStore.get(key));
    }

    @Test
    void transactionSetDelete() {
        KeyValueStore dataStore = new KeyValueStore();
        String key1 = "hello";
        String inputVal1 = "world";
        dataStore.set(key1, inputVal1);

        dataStore.begin();
        String key2 = "foo";
        String inputVal2 = "bar";
        dataStore.set(key2, inputVal2);
        assertEquals(inputVal2, dataStore.get(key2));
        dataStore.delete(key2);
        assertNull(dataStore.get(key2));
        dataStore.rollback();

        assertEquals(inputVal1, dataStore.get(key1));
        assertNull(dataStore.get(key2));
    }

    @Test
    void concurrency() throws InterruptedException {
        KeyValueStore dataStore = new KeyValueStore();
        String key1 = "hello";
        String inputVal1 = "world";
        dataStore.set(key1, inputVal1);

        Thread t1 = new Thread(() -> {
            dataStore.begin();
            dataStore.delete(key1);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            dataStore.commit();

        });
        t1.start();

        // t2 should not see the delete from t1
        Thread t2 = new Thread(() -> {
            String outputVal = dataStore.get(key1);
            assertEquals(inputVal1, outputVal);
        });
        t2.start();

        t1.join();
        t2.join();
    }

    @Test
    public void concurrentWrites() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "foo";
        String orgVal = "bar";
        dataStore.set(key, orgVal);

        String t1Val = "t1";
        String t2Val = "t2";
        Thread t1 = new Thread(() -> {
            dataStore.begin();
            assertEquals(orgVal, dataStore.get(key));

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            dataStore.set(key, t1Val);
            assertEquals(t1Val, dataStore.get(key));
            dataStore.commit();
            assertEquals(t1Val, dataStore.get(key));
        });

        Thread t2 = new Thread(() -> {
            dataStore.begin();

            assertEquals(orgVal, dataStore.get(key), "t2 should not see the value set by t1");

            dataStore.set(key, t2Val);
            assertEquals(t2Val, dataStore.get(key));
            dataStore.commit();
            assertEquals(t2Val, dataStore.get(key));
        });

        t1.start();
        t2.start();

        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(t1Val, dataStore.get(key));
    }

    // add test case for delete then add back in
    @Test
    void transactionDeleteSetCommit() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "foo";
        String val = "bar";
        dataStore.set(key, val);

        dataStore.begin();
        dataStore.delete(key);
        assertNull(dataStore.get(key));
        String newVal = "baz";
        dataStore.set(key, newVal);
        assertEquals(newVal, dataStore.get(key));
        dataStore.commit();

        assertEquals(newVal, dataStore.get(key));
    }

    @Test
    void transactionDeleteSetAbort() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "foo";
        String val = "bar";

        dataStore.set(key, val);

        dataStore.begin();

        dataStore.delete(key);
        assertNull(dataStore.get(key));

        String newVal = "baz";
        dataStore.set(key, newVal);
        assertEquals(newVal, dataStore.get(key));
        dataStore.rollback();

        assertEquals(val, dataStore.get(key));
    }
}