package core;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.io.File;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class BasicReadWriteTest {
    private SimpleKV db;
    private static final int NUM_ENTRIES = 6;
    private static final String PATH = "read_write_test";

    private static final char[][] keys = {
        {'b', 'd', 'c'},
        {'a', 'a', 'a'},
        {'b', 'a', 'a'},
        {'a', 'a', 'c'},
        {'a', 'a', 'a'}, // 4 - duplicate key
        {'a', 'b', 'a'}
    }, values = {
        {'1', '1', '1'},
        {'1', '1', '2'},
        {'1', '1', '3'},
        {'1', '1', '4'},
        {'1', '1', '5'},
        {'1', '1', '6'}
    };

    private static final char[] 
        firstOORKey = {'a', 'a'},
        firstKey = {'a', 'a', 'a'},
        middleKey1 = {'b', 'b', 'a'},
        middleKey2 = {'b', 'c', 'a'},
        middleKeyExists = {'a', 'b', 'a'},
        endKey = {'b', 'd', 'c'},
        endOORKey = {'b', 'e', 'd'};

    private static HashMap<String, ArrayList<KVPair>> rangeSols;
    static {
        rangeSols = new HashMap<String, ArrayList<KVPair>>();
        rangeSols.put("middleMiddle", new ArrayList<KVPair>(Arrays.asList(pair(5))));
        rangeSols.put("middleEnd", new ArrayList<KVPair>(Arrays.asList(pair(5), pair(2), pair(0))));
        rangeSols.put("middleEndOOR", new ArrayList<KVPair>(Arrays.asList(pair(5), pair(2), pair(0))));
        rangeSols.put("startMiddle", new ArrayList<KVPair>(Arrays.asList(pair(4), pair(3), pair(5))));
        rangeSols.put("startOORMiddle", new ArrayList<KVPair>(Arrays.asList(pair(4), pair(3), pair(5))));
        rangeSols.put("startEnd", new ArrayList<KVPair>(Arrays.asList(pair(4), pair(3), pair(5), pair(2), pair(0))));
        rangeSols.put("startOOREndOOR", new ArrayList<KVPair>(Arrays.asList(pair(4), pair(3), pair(5), pair(2), pair(0))));
        rangeSols.put("none", new ArrayList<KVPair>());
    }

    private static KVPair pair(int index) {
        return new KVPair(keys[index], values[index]);
    }

    private void assertRead(int index) {
        assertEquals(KVPair.charToString(values[index]), KVPair.charToString(this.db.read(keys[index])));
    }

    private static void compareIterators(Iterator<KVPair> got, Iterator<KVPair> exp) {
        assertEquals(iterToString(exp), iterToString(got));
    }

    private static String iterToString(Iterator<KVPair> it) {
        String result = "";
        while (it.hasNext()) {
            result += it.next().toString() + " ";
        }
        return result;
    }


    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void createDB() throws Exception {
        this.db = new SimpleKV().initAndMakeStore(PATH);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            this.db.write(keys[i], values[i]);
        }
    }

    @After
    public void destructDB() throws Exception {
        File f = new File(PATH);
        f.delete();
        f = new File(PATH + "-entries");
        f.delete();
    }

    @Test
    public void checkWrite() throws Exception {
        assertRead(0);
        assertRead(2);
        assertRead(3);
        assertRead(5);
    }

    @Test
    public void getNonExistent() throws Exception {
        assertEquals(null, this.db.read(middleKey1));
    }

    /**
     * Unit test for HeapPage.getId()
     */
    @Test
    public void checkDuplicate() throws Exception {
        assertRead(4);
        assertEquals(5, this.db.bufferSize());
    }

    @Test 
    public void writeToDisk() throws Exception {
        assertEquals(0, this.db.fileSize());
        this.db.writeToDisk();
        assertEquals(5, this.db.fileSize());
    }

    // OOR first, middle
    // first, middle
    // middle, middle
    // middle, end
    // middle, OOR end
    // OOR first, OOR end
    // first, end

    @Test
    public void testRangeSingleElement() throws Exception {
        Iterator<KVPair> it = this.db.readRange(middleKeyExists, middleKeyExists);

        compareIterators(it, rangeSols.get("middleMiddle").iterator());
    }

    @Test
    public void testRangeFirstHalfOOR() throws Exception {
        Iterator<KVPair> it = this.db.readRange(firstOORKey, middleKeyExists);

        compareIterators(it, rangeSols.get("startOORMiddle").iterator());
    }

    @Test
    public void testRangeSecondHalfOOR() throws Exception {
        Iterator<KVPair> it = this.db.readRange(middleKeyExists, endOORKey);

        compareIterators(it, rangeSols.get("middleEndOOR").iterator());
    }

    @Test
    public void testRangeFirstHalf() throws Exception {
        Iterator<KVPair> it = this.db.readRange(firstKey, middleKeyExists);

        compareIterators(it, rangeSols.get("startMiddle").iterator());
    }

    @Test
    public void testRangeSecondHalf() throws Exception {
        Iterator<KVPair> it = this.db.readRange(middleKeyExists, endKey);

        compareIterators(it, rangeSols.get("middleEnd").iterator());
    }

    @Test
    public void testRangeAllOOR() throws Exception {
        Iterator<KVPair> it = this.db.readRange(firstOORKey, endOORKey);

        compareIterators(it, rangeSols.get("startOOREndOOR").iterator());
    }

    @Test
    public void testRangeAll() throws Exception {
        Iterator<KVPair> it = this.db.readRange(firstKey, endKey);

        compareIterators(it, rangeSols.get("startEnd").iterator());
    }

    @Test
    public void testRangeNone() throws Exception {
        Iterator<KVPair> it = this.db.readRange(middleKey1, middleKey2);

        compareIterators(it, rangeSols.get("none").iterator());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(BasicReadWriteTest.class);
    }
}
