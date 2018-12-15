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

public class BufferEvictionTest {
    private SimpleKV db;
    private static final int NUM_ENTRIES = Buffer.maxSize();
    private static final String PATH = "eviction_test";

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void createDB() throws Exception {
        this.db = new SimpleKV().initAndMakeStore(PATH);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            char[] c = {(char) i};
            this.db.write(c, c);
        }
    }

    @After
    public void destructDB() throws Exception {
        File f = new File(PATH);
        f.delete();
        f = new File(PATH + "-entries");
        f.delete();
    }

    // @Test
    // public void testEviction() throws Exception {
    //     assertEquals(NUM_ENTRIES, this.db.bufferSize());
    //     assertEquals(0, this.db.fileSize());

    //     int toChar = NUM_ENTRIES;
    //     for (int i = 0; i < NUM_ENTRIES; i++) {
    //         char[] c = {(char) toChar};
    //         this.db.write(c, c);
    //         assertEquals(NUM_ENTRIES, this.db.bufferSize());
    //         assertEquals(NUM_ENTRIES, this.db.fileSize());
    //         toChar++;
    //     }

    //     char[] c = { (char) toChar };
    //     this.db.write(c, c);
    //     assertEquals(NUM_ENTRIES, this.db.bufferSize());
    //     assertEquals(NUM_ENTRIES * 2, this.db.fileSize());
    // }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(BufferEvictionTest.class);
    }
}
