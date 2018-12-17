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

public class RecoveryTest {
    private SimpleKV db;
    private static final int NUM_ENTRIES = 100;
    private static final String PATH = "recovery_test";


    private void assertRead(char[] key, char[] value) {
        assertEquals(KVPair.charToString(value), KVPair.charToString(this.db.read(key)));
    }

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void createDB() throws Exception {
        this.db = new SimpleKV().initAndMakeStore(PATH);
    }

    @After
    public void destructDB() throws Exception {
        File f = new File(PATH);
        String path = f.getAbsoluteFile().getParent();
        f.delete();
        f = new File(PATH + "-entries");
        f.delete();
        f = new File(path + "-snapshot");
        f.delete();
        f = new File(path + "-entries-snapshot");
        f.delete();
    }

    @Test
    public void testCommit() throws Exception {
        this.db.beginTx();
        // numEntries to make sure it doesnt flush to disk
        for (int i = 0; i < NUM_ENTRIES; i++) {
            char[] c = { (char) i };
            this.db.write(c, c);
        }
        this.db.commit();

        // recreate database, verify its empty
        this.db = this.db.initAndMakeStore("");
        assertEquals(0, this.db.size());

        this.db = this.db.initAndMakeStore(PATH);
        this.db.beginTx();
        assertEquals(NUM_ENTRIES, this.db.size());
    
        for (int i = 0; i < NUM_ENTRIES; i++) {
            char[] c = {(char) i};
            assertRead(c, c);
        }
        this.db.commit();
    }

    @Test
    public void testCrashNoFlush() throws Exception {
        this.db.beginTx();
        // numEntries to make sure it doesnt flush to disk
        for (int i = 0; i < NUM_ENTRIES; i++) {
            char[] c = { (char) i };
            this.db.write(c, c);
        }

        // recreate database, verify its empty
        this.db = this.db.initAndMakeStore("");
        assertEquals(0, this.db.size());

        this.db = this.db.initAndMakeStore(PATH);
        this.db.beginTx();
        assertEquals(0, this.db.size());

        for (int i = 0; i < NUM_ENTRIES; i++) {
            char[] c = { (char) i };
            assertRead(c, null);
        }
        this.db.commit();
    }

    @Test
    public void testCrashWithFlush() throws Exception {
        this.db.beginTx();
        assertEquals(0, this.db.size());
        // numEntries to make sure it flushes to disk
        for (int i = 0; i < NUM_ENTRIES; i++) {
            char[] c = { (char) i };
            this.db.write(c, c);
        }
        assertEquals(NUM_ENTRIES, this.db.size());
        this.db.writeToDisk();
        assertEquals(NUM_ENTRIES, this.db.fileSize());

        // recreate database, verify its empty
        this.db = this.db.initAndMakeStore("");
        assertEquals(0, this.db.size());

        this.db = this.db.initAndMakeStore(PATH);
        this.db.beginTx();
        assertEquals(0, this.db.size());

        for (int i = 0; i < NUM_ENTRIES; i++) {
            char[] c = { (char) i };
            assertRead(c, null);
        }
        this.db.commit();
    }

    @Test
    public void testCommitCrash() throws Exception {
        int entries = NUM_ENTRIES / 2;
        this.db.beginTx();
        // numEntries - 1 to make sure it doesnt flush to disk
        for (int i = 0; i < entries; i++) {
            char[] c = { (char) i };
            this.db.write(c, c);
        }
        this.db.commit();

        this.db = this.db.initAndMakeStore(PATH);
        this.db.beginTx();
        assertEquals(entries, this.db.size());

        for (int i = 0; i < entries; i++) {
            char[] c = { (char) i };
            char[] v = { (char) (i + 1) };
            this.db.write(c, v);
        }

        assertEquals(NUM_ENTRIES, this.db.size());

        // recreate database, verify its empty
        this.db = this.db.initAndMakeStore("");
        assertEquals(0, this.db.size());

        this.db = this.db.initAndMakeStore(PATH);
        this.db.beginTx();
        assertEquals(entries, this.db.size());

        for (int i = 0; i < entries; i++) {
            char[] c = { (char) i };
            assertRead(c, c);
        }
        this.db.commit();
    }



    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(RecoveryTest.class);
    }
}
