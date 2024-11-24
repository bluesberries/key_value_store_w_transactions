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
            System.out.println("Begin t1");
            dataStore.begin();
            /*
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            */

            dataStore.delete(key1);
            System.out.println("delete");
            dataStore.commit();
            System.out.println("Finished t1");

        });
        t1.start();

        Thread t2 = new Thread(() -> {
            System.out.println("Begin t2");
            String outputVal = dataStore.get(key1);
            System.out.println(outputVal);
            assertEquals(inputVal1, outputVal);
            System.out.println("Finished t2");
        });
        t2.start();

        t1.join();
        t2.join();
    }

    @Test
    public void concurrentWrites() {
        KeyValueStore dataStore = new KeyValueStore();
        String key = "foo";
        String val = "bar";
        dataStore.set(key, val);
        String t1Val = "t1";
        String t2Val = "t2";
        Thread t1 = new Thread(() -> {
            dataStore.begin();
            assertEquals(val, dataStore.get(key));

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            dataStore.set(key, t1Val);
            assertEquals(t1Val, dataStore.get(key));
            dataStore.commit();
        });

        Thread t2 = new Thread(() -> {
            dataStore.begin();
            assertEquals(t1Val, dataStore.get(key));

            dataStore.set(key, t2Val);
            assertEquals(t2Val, dataStore.get(key));
            dataStore.commit();
        });

        t1.start();
        t2.start();

        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}