package core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Buffer {
    private ConcurrentHashMap<String, KVPair> map;
    private ConcurrentLinkedQueue<String> queue;
    private ConcurrentHashMap<String, Boolean> dirtyEntries;
    private int numDirty, n;

    private static final int MAX_SIZE = 100;

    public Buffer() {
        this.map = new ConcurrentHashMap<String, KVPair>();
        this.queue = new ConcurrentLinkedQueue<String>();
        this.dirtyEntries = new ConcurrentHashMap<String, Boolean>();
        this.numDirty = 0;
        this.n = 0;
    }

    public KVPair get(String key) {
        return this.map.get(key);
    }

    public boolean containsKey(String key) {
        return this.map.containsKey(key);
    }

    // if using remove (only called in evict), page shouldnt be dirty
    public KVPair remove(String key) {
        n--;
        this.queue.remove(key);
        return this.map.remove(key);
    }

    public void put(String key, KVPair value, boolean dirty) {
        if (this.map.containsKey(key)) {
            this.queue.remove(key);
        } else {
            if (size() >= MAX_SIZE) evictPage();
            n++;
        }
        this.queue.add(key);
        this.map.put(key, value);

        if (!dirty || this.dirtyEntries.containsKey(key)) {
            return;
        }
        this.dirtyEntries.put(key, true);
        this.numDirty++;
    }

    public int size() {
        return this.n;
    }

    public static int maxSize() {
        return MAX_SIZE;
    }

    public int numDirtyEntries() {
        return this.numDirty;
    }

    public Iterator<KVPair> getDirtyEntries() {
        ArrayList<KVPair> entries = new ArrayList<KVPair>();
        for (String key : this.dirtyEntries.keySet()) {
            entries.add(get(key));
        }
        return entries.iterator();
    }

    public void clearDirtyEntries() {
        this.dirtyEntries = new ConcurrentHashMap<String, Boolean>();
        this.numDirty = 0;
    }

    public String findPageToEvict() {
        for (String key : this.queue) {
            if (!this.dirtyEntries.containsKey(key)) {
                return key;
            }
        }
        return null;
    }

    public void evictPage() {
        String key = this.findPageToEvict();
        if (key != null) {
            this.remove(key);
        } else {
            throw new RuntimeException("Buffer is at max capacity but no pages can be evicted.");
        }
    }
}