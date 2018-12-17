package core;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Started with implementation from Princeton, then made changes to suit my 
 * needs.
 * 
 * MAJOR CHANGES:
 * - added prev and next pointers for each of the KVPairs stored in the BTree
 *   as Entries, allowing linked list traversal between the leaf/external nodes 
 *   which is useful when traversing a range
 * - added range query functionality by creating getRange() and searchRange(), 
 *   as well as added an additional parameter (geq) to the search() function, 
 *   which allows us to toggle between searching for the exact given key and 
 *   searching for the smallest key greater than or equal to the given startRange 
 *   key (for range search)
 * - added functionality to update the value of a given key if it already exists in
 *   SimpleKV, instead of creating a new entry and storing duplicate keys
 * - changed the BTree to take in char[] objects instead of Comparables, and wrote
 *   new comparison functions for the keys
 * - made the BTree serializable
 * - converted implementation from in-menory to disk
 */

/**
 * The {@code BTree} class represents an ordered symbol table of generic
 * key-value pairs. It supports the <em>put</em>, <em>get</em>,
 * <em>contains</em>, <em>size</em>, and <em>is-empty</em> methods. A symbol
 * table implements the <em>associative array</em> abstraction: when associating
 * a value with a key that is already in the symbol table, the convention is to
 * replace the old value with the new value. Unlike {@link java.util.Map}, this
 * class uses the convention that values cannot be {@code null}—setting the
 * value associated with a key to {@code null} is equivalent to deleting the key
 * from the symbol table.
 * <p>
 * This implementation uses a B-tree. It requires that the key type implements
 * the {@code Comparable} interface and calls the {@code compareTo()} and method
 * to compare two keys. It does not call either {@code equals()} or
 * {@code hashCode()}. The <em>get</em>, <em>put</em>, and <em>contains</em>
 * operations each make log<sub><em>m</em></sub>(<em>n</em>) probes in the worst
 * case, where <em>n</em> is the number of key-value pairs and <em>m</em> is the
 * branching factor. The <em>size</em>, and <em>is-empty</em> operations take
 * constant time. Construction takes constant time.
 * <p>
 * For additional documentation, see
 * <a href="https://algs4.cs.princeton.edu/62btree">Section 6.2</a> of
 * <i>Algorithms, 4th Edition</i> by Robert Sedgewick and Kevin Wayne.
 */
public class BTree {
    private static final int BOOL_SIZE = 1, INT_SIZE = 4, LONG_SIZE = 8, PAGE_SIZE = 4096;
    private static final int FILE_HEADER_SIZE = 4 * INT_SIZE, ENTRY_HEADER_SIZE = 2 * INT_SIZE;
    private File file, entryFile;
    

    // max children per B-tree node = M-1
    // (must be even and greater than 2)
    private static final int M = 4;

    private int height; // height of the B-tree
    private int n; // number of key-value pairs in the B-tree
    private int rootPos = -1;
    private int pages = 1;

    // helper B-tree node data type
    private static final class Node {
        private int m, pageNo; // number of children
        private int[] children = new int[M];
        private Entry[] entries = new Entry[M]; // the array of children

        // create an empty node with k children
        private Node(int k, int p) {
            m = k;
            pageNo = p;
        }

        private Node(byte[] nodeData) {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(nodeData));

            try {
                pageNo = dis.readInt();
                m = dis.readInt();

                for (int i = 0; i < m; i++) {
                    children[i] = dis.readInt();
    
                    int length = dis.readInt();
                    byte[] entry = new byte[length];
                    dis.read(entry, 0, length);
                    
                    entries[i] = new Entry(entry);
                }

                dis.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } 

        private byte[] serialize() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(PAGE_SIZE);
            DataOutputStream dos = new DataOutputStream(baos);
            int entriesSize = 0;
            try {
                dos.writeInt(pageNo);
                dos.writeInt(m);

                for (int i = 0; i < m; i++) {
                    dos.writeInt(children[i]);
                    dos.writeInt(entries[i].size);
                    dos.write(entries[i].serialize());
                    entriesSize += entries[i].size;
                }

                int padding = PAGE_SIZE - (2 * INT_SIZE) - (2 * m * INT_SIZE) - entriesSize;

                for (int i = 0; i < padding; i++) {
                    dos.write(0);
                }

                dos.flush();
                dos.close();
                return baos.toByteArray();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);

            }
        }

        private long startOfEntries() {
            return (PAGE_SIZE * pageNo + FILE_HEADER_SIZE) + (2 * INT_SIZE) + ENTRY_HEADER_SIZE;
        }
    }

    // internal nodes: only use key and nextN
    // external nodes: only use key, value, next, prev
    public static class Entry implements Serializable{
        public static final long serialVersionUID = 1L;
        public char[] key;
        public int size;
        public long val, prev, next, offset;
        public boolean isExternal;

        private Entry(char[] key, long val, long prev, long next, long offset, boolean external) {
            this.key = key;
            this.val = val; // -1 if internal
            this.prev = prev; // -1 if internal
            this.next = next; // -1 if internal
            this.offset = offset;
            this.isExternal = external;
            this.size = size();
        }

        private Entry(byte[] entryData) {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(entryData));

            try {
                size = dis.readInt();
                offset = dis.readLong();
                isExternal = dis.readBoolean();
                int length = dis.readInt();
                key = new char[length];

                for (int j = 0; j < length; j++) {
                    key[j] = dis.readChar();
                }
                
                if (isExternal) {
                    val = dis.readLong();
                    prev = dis.readLong();
                    next = dis.readLong();
                } else {
                    val = -1;
                    prev = -1;
                    next = -1;
                }

                dis.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        private byte[] serialize() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
            DataOutputStream dos = new DataOutputStream(baos);

            try {
                dos.writeInt(size);
                dos.writeLong(offset);
                dos.writeBoolean(isExternal); // 1 if this is an external node
                dos.writeInt(key.length);
                
                for (int i = 0; i < key.length; i++) {
                    dos.writeChar(key[i]);
                }

                if (isExternal) {
                    dos.writeLong(val);
                    dos.writeLong(prev);
                    dos.writeLong(next);
                }

                dos.flush();
                dos.close();
                return baos.toByteArray();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);

            }    
        }

        private int size() {
            int size = BOOL_SIZE + LONG_SIZE + (2 * INT_SIZE) + (key.length * 2) + (isExternal ? (3 * LONG_SIZE) : 0);
            return size; 
        }
    }

    /**
     * Initializes an empty B-tree.
     */
    public BTree(File f, File ef) {
        file = f;
        entryFile = f;
        rereadValues();
    }

    public void rereadValues() {
        try {
            RandomAccessFile bf = new RandomAccessFile(file, "r");
            rootPos = bf.readInt();
            pages = bf.readInt();
            n = bf.readInt();
            height = bf.readInt();
            bf.close();

            System.out.println("initial eof: " + getEndOfValues());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] emptyTreeData() {
        int size = FILE_HEADER_SIZE + PAGE_SIZE;

        ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            dos.writeInt(0);
            dos.writeInt(1);
            dos.writeInt(0);
            dos.writeInt(0);
            dos.write(new Node(0, 0).serialize());
            dos.flush();
            dos.close();

            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] emptyEntryData() {
        int size = LONG_SIZE;

        ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            dos.writeLong(LONG_SIZE);
            dos.flush();
            dos.close();

            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static int nodeSize(Node n) {
        int size = 2 * INT_SIZE;
        for (int i = 0; i < n.m; i++) {
            size += n.entries[i].size + ENTRY_HEADER_SIZE;
        }
        return size;
    }

    /**
     * Returns true if this symbol table is empty.
     * 
     * @return {@code true} if this symbol table is empty; {@code false} otherwise
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the number of key-value pairs in this symbol table.
     * 
     * @return the number of key-value pairs in this symbol table
     */
    public int size() {
        return n;
    }

    /**
     * Returns the height of this B-tree (for debugging).
     *
     * @return the height of this B-tree
     */
    public int height() {
        return height;
    }

    private int getRootPageNo() {
        try {
            RandomAccessFile f = new RandomAccessFile(file, "r");
            rootPos = f.readInt();
            f.close();
            return rootPos;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private long getEndOfValues() {
        try {
            RandomAccessFile f = new RandomAccessFile(entryFile, "r");
            long end = f.readLong();
            f.close();
            return end;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Node readRoot() {
        try {
            return readNode(getRootPageNo());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Node readNode(int pno) {
        try {
            RandomAccessFile f = new RandomAccessFile(file, "r");
            f.seek(pno * PAGE_SIZE + FILE_HEADER_SIZE);

            byte[] pageData = new byte[PAGE_SIZE];
            f.read(pageData, 0, PAGE_SIZE);
            f.close();
            return new Node(pageData);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Entry readEntry(long pos) {
        try {
            RandomAccessFile f = new RandomAccessFile(file, "r");
            f.seek(pos);

            int length = f.readInt();
            byte[] entryData = new byte[length];
            f.read(entryData, 0, length);
            f.close();
            Entry e = new Entry(entryData);
            
            return e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public char[] readValue(Entry entry) {
        if (entry == null) return null;
        
        try {
            RandomAccessFile f = new RandomAccessFile(entryFile, "r");
            System.out.println("entry: " + KVPair.charToString(entry.key) + " " + entry.val);
            f.seek(entry.val);
            
            int length = f.readInt();
            char[] val = new char[length];
            for (int i = 0; i < length; i++) {
                val[i] = f.readChar();
            }
            f.close();

            return val;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private KVPair readKVPair(Entry entry) {
        char[] val = readValue(entry);
        if (val == null) return null;

        return new KVPair(entry.key, val);
    }

    /**
     * Returns the value associated with the given key.
     *
     * @param key the key
     * @return the value associated with the given key if the key is in the symbol
     *         table and {@code null} if the key is not in the symbol table
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public char[] get(char[] key) {
        if (key == null)
            throw new IllegalArgumentException("argument to get() is null");
        Node root = readRoot();
        return readValue(search(root, key, height, false));
    }

    public Iterator<KVPair> getRange(char[] k1, char[] k2) {
        if (k1 == null || k2 == null)
            throw new IllegalArgumentException("argument(s) to getRange() is null");
        Node root = readRoot();
        return searchRange(root, k1, k2);
    }

    // if geq = true, return the first element greater than or equal to the key
    private Entry search(Node x, char[] key, int ht, boolean geq) {
        Entry[] entries = x.entries;
        System.out.println("searching " + ht + " " + x.m + " " + x.pageNo);

        if (ht != 0) {
            for (int j = 0; j < x.m; j++) {
                if (j + 1 == x.m || less(key, entries[j + 1].key))
                    return search(readNode(x.children[j]), key, ht - 1, geq);
            }
        } else {
            for (int j = 0; j < x.m; j++) {
                if (geq ? geq(entries[j].key, key) : eq(key, entries[j].key))
                    return entries[j];
            }
            // must be greater than key because its parent node is greater than key

            if (geq && entries[x.m - 1].next != -1) return readEntry(entries[x.m - 1].next);
        }
        return null;
    }

    private Iterator<KVPair> searchRange(Node x, char[] k1, char[] k2) {
        Node root = readRoot();
        Entry entry = search(root, k1, height, true);
        if (entry == null || more(entry.key, k2)) {
            return Collections.emptyIterator();
        }

        ArrayList<KVPair> values = new ArrayList<KVPair>();
        values.add(readKVPair(entry));
        while (entry.next != -1) {
            entry = readEntry(entry.next);
            if (more(entry.key, k2)) break;
            values.add(readKVPair(entry));
        }

        return values.iterator();
    }

    /**
     * Inserts the key-value pair into the symbol table, overwriting the old value
     * with the new value if the key is already in the symbol table. If the value is
     * {@code null}, this effectively deletes the key from the symbol table.
     *
     * @param key the key
     * @param val the value
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public void put(char[] key, char[] val) {
        if (key == null)
            throw new IllegalArgumentException("argument key to put() is null");
        Node root = readRoot();
        //System.out.println("root node " + root.pageNo + " " + root.m);
        ArrayList<Entry> affectedEntries = new ArrayList<Entry>();
        ArrayList<Node> affectedNodes = new ArrayList<Node>();

        Node u = insert(root, key, val, height, affectedEntries, affectedNodes);
        n++;
        
        rootPos = -1;
        if (u != null) {
            // need to split root
            Node t = new Node(2, pages);
            pages++;
            t.children[0] = getRootPageNo();
            t.children[1] = u.pageNo;
            t.entries[0] = new Entry(root.entries[0].key, -1, -1, -1, t.startOfEntries(), false);
            t.entries[1] = new Entry(u.entries[0].key, -1, -1, -1, t.startOfEntries() + ENTRY_HEADER_SIZE + t.entries[0].size, false);
            height++;

            // System.out.println("new root");
            rootPos = t.pageNo;
            affectedNodes.add(t);
        }

        try {
            RandomAccessFile f = new RandomAccessFile(entryFile, "rw");
            long pos = getEndOfValues(); // f.readLong();
            
            f.seek(pos);
            f.writeInt(val.length);

            for (int i = 0; i < val.length; i++) {
                f.writeChar(val[i]);
            }
            
            long newEnd = f.getFilePointer();
            System.out.println("eof pos: " + pos + " new eof pos: " + f.getFilePointer());
            f.seek(0);
            f.writeLong(newEnd);
            f.seek(0);
            System.out.println("new eof pos read: " + f.readLong());
            f.close();

            f = new RandomAccessFile(file, "rw");
            if (rootPos != -1) {
                // System.out.println("writing new root");
                f.seek(0);
                f.writeInt(rootPos);
            } else {
                f.seek(INT_SIZE);
            }
            f.writeInt(pages);
            f.writeInt(n);
            f.writeInt(height);
            for (Entry e : affectedEntries) {
                f.seek(e.offset);
                f.write(e.serialize());
            }

            for (Node n : affectedNodes) {
                //System.out.println("affected node " + n.pageNo + " " + n.m);
                f.seek(n.pageNo * PAGE_SIZE + FILE_HEADER_SIZE);
                f.write(n.serialize());
            }
            f.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // maybe have an array of affected nodes
    private Node insert(Node h, char[] key, char[] val, int ht, ArrayList<Entry> affectedEntries, ArrayList<Node> affectedNodes) {
        int j, pno = -1;
        Entry t = new Entry(key, -1, -1, -1, -1, (ht == 0));
        boolean alreadyExists = false;

        // System.out.println("inserting " + ht);
        // external node
        if (ht == 0) {
            for (j = 0; j < h.m; j++) {
                if (less(key, h.entries[j].key))
                    break;
                if (eq(key, h.entries[j].key)) {
                    alreadyExists = true;
                    break;
                }
            }
        }

        // internal node
        else {
            for (j = 0; j < h.m; j++) {
                if ((j + 1 == h.m) || less(key, h.entries[j + 1].key)) {
                    Node u = insert(readNode(h.children[j++]), key, val, ht - 1, affectedEntries, affectedNodes);
                    if (u == null)
                        return null;
                    t.key = u.entries[0].key;
                    pno = u.pageNo;
                    System.out.println("new page no: " + pno);
                    break;
                }
            }
        }

        if (alreadyExists) {
            h.entries[j].val = getEndOfValues();
            n--;
            affectedEntries.add(h.entries[j]);
            return null;
        }
        
        if (ht == 0) {
            t.val = getEndOfValues();
            System.out.println("setting val pointer to " + t.val);
        } 

        long offset = (h.m == 0) ? h.startOfEntries() : h.entries[h.m - 1].offset + h.entries[h.m - 1].size + ENTRY_HEADER_SIZE;
        for (int i = h.m; i > j; i--) {
            h.entries[i] = h.entries[i - 1];
            h.children[i] = h.children[i - 1];
            if (i - 1 == j) {
                offset = h.entries[i].offset;
                h.entries[i].offset += t.size + (2 * INT_SIZE);
            } else {
                h.entries[i].offset += h.entries[i - 2].size + (2 * INT_SIZE);
            }
        }

        t.offset = offset;
        h.entries[j] = t;
        h.m++;

        if (pno != -1) h.children[j] = pno;
        if (ht == 0) {
            if (j == 0) {
                if (h.m > 1) {
                    h.entries[j].next = h.entries[j + 1].offset;
                    h.entries[j].prev = h.entries[j + 1].prev;
                    h.entries[j + 1].prev = h.entries[j].offset;
                }
                
                if (h.entries[j].prev != -1) {
                    Entry e = readEntry(h.entries[j].prev);
                    e.next = h.entries[j].offset;
                    affectedEntries.add(e);
                }
            } else if (j == h.m - 1) {
                h.entries[j].next = h.entries[j - 1].next;
                h.entries[j].prev = h.entries[j - 1].offset;

                h.entries[j - 1].next = h.entries[j].offset;

                if (h.entries[j].next != -1) {
                    Entry e = readEntry(h.entries[j].next);
                    e.prev = h.entries[j].offset;
                    affectedEntries.add(e);
                } 
            } else {
                h.entries[j].next = h.entries[j + 1].offset;
                h.entries[j].prev = h.entries[j - 1].offset;

                h.entries[j - 1].next = h.entries[j].offset;

                h.entries[j + 1].prev = h.entries[j].offset;
            }
        }
        
        affectedNodes.add(h);
        if (h.m < M && nodeSize(h) < PAGE_SIZE) {
            return null;
        } else {
            // System.out.println("splitting");
            Node newNode = split(h);
            affectedNodes.add(newNode);
            return newNode;
        }
    }

    // split node in half
    private Node split(Node h) {
        int hSize = h.m / 2, tSize = h.m / 2;
        if (h.m % 2 != 0) {
            tSize += 1;
        }

        Node t = new Node(tSize, pages);
        pages++;
        h.m = hSize;
        long offset = t.startOfEntries();

    
        // int lastIndex = M / 2 - 1;
        for (int j = 0; j < tSize; j++) {
            boolean isNotLast = j + 1 != tSize;
            t.entries[j] = h.entries[hSize + j];
            t.entries[j].offset = offset;

            if (j == 0) h.entries[hSize - 1].next = offset;
            t.children[j] = h.children[hSize + j];
            if (isNotLast) h.entries[hSize + j + 1].prev = offset;

            offset += t.entries[j].size + (2 * INT_SIZE);
            if (isNotLast) t.entries[j].next = offset;
            // h.children[lastIndex].next = t.children[0];
            // t.children[0].prev = h.children[lastIndex];
        }
            
        return t;
    }

    /**
     * Returns a string representation of this B-tree (for debugging).
     *
     * @return a string representation of this B-tree.
     */
    public String toString() {
        return toString(readRoot(), height, "") + "\n";
    }

    private String toString(Node h, int ht, String indent) {
        StringBuilder s = new StringBuilder();
        Entry[] entries = h.entries;

        if (ht == 0) {
            for (int j = 0; j < h.m; j++) {
                s.append(indent + KVPair.charToString(entries[j].key) + " " +  KVPair.charToString(readValue(entries[j])) + "\n");
            }
        } else {
            for (int j = 0; j < h.m; j++) {
                if (j > 0)
                    s.append(indent + "(" +  KVPair.charToString(entries[j].key) + ")\n");
                s.append(toString(readNode(h.children[j]), ht - 1, indent + "     "));
            }
        }
        return s.toString();
    }

    private boolean less(char[] k1, char[] k2) {
        int diff = k1.length - k2.length;
        for (int i = 0; i < Math.min(k1.length, k2.length); i++) {
            int less = k1[i] - k2[i];
            if (less > 0) return false;
            if (less < 0) return true;
        }
        return diff < 0;
    }

    private boolean more(char[] k1, char[] k2) {
        int diff = k1.length - k2.length;
        for (int i = 0; i < Math.min(k1.length, k2.length); i++) {
            int more = k1[i] - k2[i];
            if (more > 0)
                return true;
            if (more < 0)
                return false;
        }
        return diff > 0;
    }

    private boolean eq(char[] k1, char[] k2) {
        int diff = k1.length - k2.length;
        if (diff != 0) return false;

        for (int i = 0; i < Math.min(k1.length, k2.length); i++) {
            int eq = k1[i] - k2[i];
            if (eq != 0) return false;
        }
        return true;
    }

    private boolean geq(char[] k1, char[] k2) {
        int diff = k1.length - k2.length;
        for (int i = 0; i < Math.min(k1.length, k2.length); i++) {
            int more = k1[i] - k2[i];
            if (more > 0)
                return true;
            if (more < 0)
                return false;
        }
        return diff >= 0;
    }
}