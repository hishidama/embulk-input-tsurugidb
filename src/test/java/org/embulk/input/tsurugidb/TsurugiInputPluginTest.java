package org.embulk.input.tsurugidb;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.hishidama.embulk.tester.EmbulkTestOutputPlugin.OutputRecord;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class TsurugiInputPluginTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name
    private static final int SIZE = 4;

    @BeforeClass
    public static void beforeAll() {
        dropTable(TEST);

        String sql = "create table " + TEST //
                + "(" //
                + " pk int primary key," //
                + " long_value bigint," //
                + " double_value double," //
                + " string_value varchar(10)" //
                + ")";
        createTable(sql);

        insertTable();
    }

    private static void insertTable() {
        String sql = "insert into " + TEST //
                + "(pk, long_value, double_value, string_value)" //
                + " values(:pk, :long_value, :double_value, :string_value)";
        var placeholders = List.of(placeholder("pk", AtomType.INT4), //
                placeholder("long_value", AtomType.INT8), //
                placeholder("double_value", AtomType.FLOAT8), //
                placeholder("string_value", AtomType.CHARACTER));
        executeOcc((sqlClient, transaction) -> {
            try (var ps = prepare(sqlClient, sql, placeholders)) {
                for (int i = 0; i < SIZE; i++) {
                    var parameters = List.of(parameter("pk", i), //
                            parameter("long_value", 100L - i), //
                            parameter("double_value", i / 10d), //
                            parameter("string_value", "z" + i));
                    transaction.executeStatement(ps, parameters);
                }
            }
        });
    }

    @Test
    public void testQuery() {
        try (var tester = new EmbulkPluginTester()) {
            tester.addInputPlugin(TsurugiInputPlugin.TYPE, TsurugiInputPlugin.class);

            var in = tester.newConfigSource("in");
            in.set("type", TsurugiInputPlugin.TYPE);
            in.set("endpoint", ENDPOINT);
            setCredential(in);
            in.set("query", "select * from " + TEST + " order by pk");

            List<OutputRecord> result = tester.runInput(in);
            assertEquals(SIZE, result.size());
            int i = 0;
            for (var record : result) {
                assertEquals(Long.valueOf(i), record.getAsLong("pk"));
                assertEquals(Long.valueOf(100L - i), record.getAsLong("long_value"));
                assertEquals(Double.valueOf(i / 10d), record.getAsDouble("double_value"));
                assertEquals("z" + i, record.getAsString("string_value"));
                i++;
            }
        }
    }

    @Test
    public void testTable() {
        try (var tester = new EmbulkPluginTester()) {
            tester.addInputPlugin(TsurugiInputPlugin.TYPE, TsurugiInputPlugin.class);

            int size = SIZE - 1;

            var in = tester.newConfigSource("in");
            in.set("type", TsurugiInputPlugin.TYPE);
            in.set("endpoint", ENDPOINT);
            setCredential(in);
            in.set("table", TEST);
            in.set("where", "pk < " + size);
            in.set("order_by", "long_value");

            List<OutputRecord> result = tester.runInput(in);
            assertEquals(size, result.size());
            int i = size - 1;
            for (var record : result) {
                assertEquals(Long.valueOf(i), record.getAsLong("pk"));
                assertEquals(Long.valueOf(100L - i), record.getAsLong("long_value"));
                assertEquals(Double.valueOf(i / 10d), record.getAsDouble("double_value"));
                assertEquals("z" + i, record.getAsString("string_value"));
                i--;
            }
        }
    }

    @Test
    public void testNoColumnName() {
        try (var tester = new EmbulkPluginTester()) {
            tester.addInputPlugin(TsurugiInputPlugin.TYPE, TsurugiInputPlugin.class);

            var in = tester.newConfigSource("in");
            in.set("type", TsurugiInputPlugin.TYPE);
            in.set("endpoint", ENDPOINT);
            setCredential(in);
            in.set("query", "select count(*), max(long_value) from " + TEST);

            List<OutputRecord> result = tester.runInput(in);
            assertEquals(1, result.size());
            var record = result.get(0);
            assertEquals(Long.valueOf(SIZE), record.getAsLong("@#0"));
            assertEquals(Long.valueOf(100L), record.getAsLong("@#1"));
        }
    }

    @Test
    @Ignore
    public void testScanDefault() {
        try (var tester = new EmbulkPluginTester()) {
            tester.addInputPlugin(TsurugiInputPlugin.TYPE, TsurugiInputPlugin.class);

            int size = SIZE - 1;

            var in = tester.newConfigSource("in");
            in.set("type", TsurugiInputPlugin.TYPE);
            in.set("endpoint", ENDPOINT);
            setCredential(in);
            in.set("table", TEST);
            in.set("method", "scan");

            List<OutputRecord> result = tester.runInput(in);
            assertEquals(size, result.size());
            int i = size - 1;
            for (var record : result) {
                assertEquals(Long.valueOf(i), record.getAsLong("pk"));
                assertEquals(Long.valueOf(100L - i), record.getAsLong("long_value"));
                assertEquals(Double.valueOf(i / 10d), record.getAsDouble("double_value"));
                assertEquals("z" + i, record.getAsString("string_value"));
                i--;
            }
        }
    }

    @Test
    @Ignore
    public void testScanOcc() {
        try (var tester = new EmbulkPluginTester()) {
            tester.addInputPlugin(TsurugiInputPlugin.TYPE, TsurugiInputPlugin.class);

            int size = SIZE - 1;

            var in = tester.newConfigSource("in");
            in.set("type", TsurugiInputPlugin.TYPE);
            in.set("endpoint", ENDPOINT);
            setCredential(in);
            in.set("table", TEST);
            in.set("method", "scan");
            in.set("tx_type", "OCC");

            List<OutputRecord> result = tester.runInput(in);
            assertEquals(size, result.size());
            int i = size - 1;
            for (var record : result) {
                assertEquals(Long.valueOf(i), record.getAsLong("pk"));
                assertEquals(Long.valueOf(100L - i), record.getAsLong("long_value"));
                assertEquals(Double.valueOf(i / 10d), record.getAsDouble("double_value"));
                assertEquals("z" + i, record.getAsString("string_value"));
                i--;
            }
        }
    }
}
