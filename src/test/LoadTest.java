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

public class LoadTest {
    private SimpleKV db;
    private static final int NUM_ENTRIES = Buffer.maxSize();
    private static final String PATH = "load_test";


    private void assertRead(char[] key, char[] value) {
        assertEquals(KVPair.charToString(value), KVPair.charToString(this.db.read(key)));
    }

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
        this.db.writeToDisk();
    }

    @After
    public void destructDB() throws Exception {
        File f = new File(PATH);
        f.delete();
        f = new File(PATH + "-entries");
        f.delete();
    }

    // @Test
    // public void testLoad() throws Exception {
    //     assertEquals(NUM_ENTRIES, this.db.fileSize());

    //     // recreate database, verify its empty
    //     this.db = this.db.initAndMakeStore("");
    //     assertEquals(0, this.db.fileSize());
    //     assertEquals(0, this.db.bufferSize());

    //     this.db = this.db.initAndMakeStore(PATH);
    //     assertEquals(NUM_ENTRIES, this.db.fileSize());
    
    //     for (int i = 0; i < NUM_ENTRIES; i++) {
    //         char[] c = {(char) i};
    //         assertRead(c, c);
    //     }
    // }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(LoadTest.class);
    }
}
