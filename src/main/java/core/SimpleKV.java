package core;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
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
        try {
            // TODO: check if snapshot exists = means crash, fix copy error
            File f = new File(filePath);
            File copy = new File(f.getAbsoluteFile().getParent() + "-snapshot");
            File ef = new File(filePath + "-entries");
            File ecopy = new File(f.getAbsoluteFile().getParent() + "-entries-snapshot");

            if (!copy.createNewFile()) {
                Files.copy(copy.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING);

                Files.copy(ecopy.toPath(), ef.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.copy(f.toPath(), copy.toPath(), StandardCopyOption.REPLACE_EXISTING);

                Files.copy(ef.toPath(), ecopy.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } 
    }

    @Override
    public void commit() {
        writeToDisk();
        File f = new File(filePath + "-snapshot");
        f.delete();

        f = new File(filePath + "-entries-snapshot");
        f.delete();
    }

}
