package core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Iterator;

import javax.xml.namespace.QName;

public class SimpleKV implements KeyValue {
    private BTree tree;
    private Buffer buffer;
    private String filePath;

    private static final int MAX_WRITES = Buffer.maxSize();

    public SimpleKV() {
        buffer = new Buffer();
        filePath = "simpleKVStore";
        createFiles(filePath);
    }

    public SimpleKV(String file) {
        buffer = new Buffer();
        filePath = file;
        createFiles(filePath);
    }

    private void createFiles(String filePath) {
        try {
            File f = new File(filePath);
            if (f.createNewFile()) {
                RandomAccessFile rf = new RandomAccessFile(filePath, "rw");
                rf.write(BTree.emptyTreeData());
                rf.close();
            }  

            String entryPath = filePath + "-entries";
            File ef = new File(entryPath);
            if (ef.createNewFile()) {
                System.out.println("new entry file");
                RandomAccessFile rf = new RandomAccessFile(entryPath, "rw");
                rf.setLength(4096);
                rf.write(BTree.emptyEntryData());
                rf.close();
            }
            tree = new BTree(f, ef);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Database could not be created: " + e.toString());
        } 
    }

    // maybe add to buffer?
    // public SimpleKV(Object t) {
    //     tree = (BTree) t;
    //     buffer = new Buffer();
    // }

    @Override
    public SimpleKV initAndMakeStore(String path) {
        
        if (!path.isEmpty()) {
            return new SimpleKV(path);
        }
        return new SimpleKV();
    }

    public void writeToDisk() {
        Iterator<KVPair> entries = buffer.getDirtyEntries();
        while (entries.hasNext()) {
            KVPair pair = entries.next();
            this.tree.put(pair.element1, pair.element2);
        }

        try {
            buffer.clearDirtyEntries();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("BTree could not be written to disk: " + e.toString());
        }
    }

    public int fileSize() {
        return this.tree.size();
    }

    public int bufferSize() {
        return this.buffer.size();
    }

    @Override
    public void write(char[] key, char[] value) {
        if (buffer.numDirtyEntries() >= MAX_WRITES) {
            writeToDisk();
        }
        this.buffer.put(KVPair.charToString(key), new KVPair(key, value), true);
    }

    @Override
    public char[] read(char[] key) {
        KVPair value = this.buffer.get(KVPair.charToString(key));
        
        if (value != null) {
            return value.element2;
        }
        char[] diskVal = this.tree.get(key);
        if (diskVal != null) this.buffer.put(KVPair.charToString(key), new KVPair(key, diskVal), false);
        return diskVal;
    }

    @Override
    public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
        writeToDisk();
        System.out.println(this.tree.toString());
        return this.tree.getRange(startKey, endKey);
        // add to buffer
        // if (range != null) {
            
        // }
    }

    @Override
    public void beginTx() {
	    System.out.println("Done!");
    }

    @Override
    public void commit() {
	    System.out.println("Done!");
    }

}
