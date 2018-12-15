package core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.io.IOException;
import java.lang.ClassNotFoundException;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import core.KVPair;

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
public class BTreeCopy implements Serializable {
    private static final long serialVersionUID = 1L;


    // max children per B-tree node = M-1
    // (must be even and greater than 2)
    private static final int M = 4;

    private Node root; // root of the B-tree
    private int height; // height of the B-tree
    private int n; // number of key-value pairs in the B-tree

    // helper B-tree node data type
    private static final class Node implements Serializable {
        private static final long serialVersionUID = 1L;
        private int m; // number of children
        private Entry[] children = new Entry[M]; // the array of children

        // create a node with k children
        private Node(int k) {
            m = k;
        }
    }

    // internal nodes: only use key and nextN
    // external nodes: only use key, value, next, prev
    private static class Entry implements Serializable{
        private static final long serialVersionUID = 1L;
        private char[] key;
        private final char[] val;
        private Node nextN; // helper field to iterate over array entries
        private Entry prev;
        private Entry next;

        public Entry(char[] key, char[] val, Node nextN, Entry prev, Entry next) {
            this.key = key;
            this.val = val;
            this.nextN = nextN;
            this.prev = prev;
            this.next = next;
        }
    }

    /**
     * Initializes an empty B-tree.
     */
    public BTreeCopy() {
        root = new Node(0);
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

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
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
        Entry result = search(root, key, height, false);
        if (result == null) return null;
        return result.val;
    }

    public Iterator<KVPair> getRange(char[] k1, char[] k2) {
        if (k1 == null || k2 == null)
            throw new IllegalArgumentException("argument(s) to getRange() is null");
        return searchRange(root, k1, k2);
    }

    // if geq = true, return the first element greater than or equal to the key
    private Entry search(Node x, char[] key, int ht, boolean geq) {
        Entry[] children = x.children;
        Entry result;

        if (ht != 0) {
            for (int j = 0; j < x.m; j++) {
                if (j + 1 == x.m || less(key, children[j + 1].key))
                    return search(children[j].nextN, key, ht - 1, geq);
            }
        } else {
            for (int j = 0; j < x.m; j++) {
                if (geq ? geq(children[j].key, key) : eq(key, children[j].key))
                    return children[j];
            }
            // must be greater than key because its parent node is greater than key
            if (geq) return children[x.m - 1].next;
        }
        return null;
    }

    private Iterator<KVPair> searchRange(Node x, char[] k1, char[] k2) {
        Entry entry = search(root, k1, height, true);
        if (entry == null || more(entry.key, k2)) {
            return Collections.emptyIterator();
        }

        ArrayList<KVPair> values = new ArrayList<KVPair>();
        values.add(new KVPair(entry.key, entry.val));
        while (entry.next != null && !more(entry.next.key, k2)) {
            entry = entry.next;
            values.add(new KVPair(entry.key, entry.val));
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
        Node u = insert(root, key, val, height);
        n++;
        if (u == null)
            return;

        // need to split root
        Node t = new Node(2);
        t.children[0] = new Entry(root.children[0].key, null, root, null, null);
        t.children[1] = new Entry(u.children[0].key, null, u, null, null);
        root = t;
        height++;
    }

    private Node insert(Node h, char[] key, char[] val, int ht) {
        int j;
        Entry t = new Entry(key, val, null, null, null);
        boolean alreadyExists = false;

        // external node
        if (ht == 0) {
            for (j = 0; j < h.m; j++) {
                if (less(key, h.children[j].key))
                    break;
                if (eq(key, h.children[j].key)) {
                    alreadyExists = true;
                    break;
                }
            }
        }

        // internal node
        else {
            for (j = 0; j < h.m; j++) {
                if ((j + 1 == h.m) || less(key, h.children[j + 1].key)) {
                    Node u = insert(h.children[j++].nextN, key, val, ht - 1);
                    if (u == null)
                        return null;
                    t.key = u.children[0].key;
                    t.nextN = u;
                    break;
                }
            }
        }

        if (alreadyExists) {
            if (h.children[j].next != null) {
                t.next = h.children[j].next;
                h.children[j].next.prev = t;
            }
            
            if (h.children[j].prev != null) {
                t.prev = h.children[j].prev;
                h.children[j].prev = t;
            }
            
            h.children[j] = t;
            n--;
            return null;
        } 
        for (int i = h.m; i > j; i--)
            h.children[i] = h.children[i - 1];
        h.children[j] = t;
        h.m++;
        if (ht == 0) {
            if (j == 0) {
                if (h.m > 1) {
                    h.children[j].next = h.children[j + 1];
                    h.children[j].prev = h.children[j + 1].prev;
                    h.children[j + 1].prev = h.children[j];
                }
                
                if (h.children[j].prev != null)
                    h.children[j].prev.next = h.children[j];
            } else if (j == h.m - 1) {
                h.children[j].next = h.children[j - 1].next;
                h.children[j].prev = h.children[j - 1];

                h.children[j - 1].next = h.children[j];

                if (h.children[j].next != null)
                    h.children[j].next.prev = h.children[j];
            } else {
                h.children[j].next = h.children[j + 1];
                h.children[j].prev = h.children[j - 1];

                h.children[j - 1].next = h.children[j];

                h.children[j + 1].prev = h.children[j];
            }
        }
        
        if (h.m < M)
            return null;
        else
            return split(h);
    }

    // split node in half
    private Node split(Node h) {
        Node t = new Node(M / 2);
        h.m = M / 2;
        // int lastIndex = M / 2 - 1;
        for (int j = 0; j < M / 2; j++)
            t.children[j] = h.children[M / 2 + j];
        // h.children[lastIndex].next = t.children[0];
        // t.children[0].prev = h.children[lastIndex];
        return t;
    }

    /**
     * Returns a string representation of this B-tree (for debugging).
     *
     * @return a string representation of this B-tree.
     */
    public String toString() {
        return toString(root, height, "") + "\n";
    }

    private String toString(Node h, int ht, String indent) {
        StringBuilder s = new StringBuilder();
        Entry[] children = h.children;

        if (ht == 0) {
            for (int j = 0; j < h.m; j++) {
                s.append(indent + KVPair.charToString(children[j].key) + " " +  KVPair.charToString(children[j].val) + "\n");
            }
        } else {
            for (int j = 0; j < h.m; j++) {
                if (j > 0)
                    s.append(indent + "(" +  KVPair.charToString(children[j].key) + ")\n");
                s.append(toString(children[j].nextN, ht - 1, indent + "     "));
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