package org.embulk.input.tsurugidb;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.hishidama.embulk.tester.EmbulkTestOutputPlugin.OutputRecord;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class SessionKeepAliveTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name
    private static final int SIZE = 4;

    @BeforeClass
    public static void beforeAll() {
        dropTable(TEST);

        String sql = "create table " + TEST //
                + "(" //
                + " pk int primary key," //
                + " string_value varchar(10)" //
                + ")";
        createTable(sql);

        insertTable();
    }

    private static void insertTable() {
        String sql = "insert into " + TEST //
                + "(pk, string_value)" //
                + " values(:pk, :string_value)";
        var placeholders = List.of(placeholder("pk", AtomType.INT4), //
                placeholder("string_value", AtomType.CHARACTER));
        executeOcc((sqlClient, transaction) -> {
            try (var ps = prepare(sqlClient, sql, placeholders)) {
                for (int i = 0; i < SIZE; i++) {
                    var parameters = List.of(parameter("pk", i), //
                            parameter("string_value", "z" + i));
                    transaction.executeStatement(ps, parameters);
                }
            }
        });
    }

    @Test
    public void testDefault() {
        test(null);
    }

    @Test
    public void testTrue() {
        test(true);
    }

    @Test
    public void testFalse() {
        test(false);
    }

    private void test(Boolean keepAlive) {
        try (var tester = new EmbulkPluginTester()) {
            tester.addInputPlugin(TsurugiInputPlugin.TYPE, TsurugiInputPlugin.class);

            var in = tester.newConfigSource("in");
            in.set("type", TsurugiInputPlugin.TYPE);
            in.set("endpoint", ENDPOINT);
            in.set("table", TEST);
            if (keepAlive != null) {
                in.set("session_keep_alive", keepAlive.toString());
            }

            List<OutputRecord> result = tester.runInput(in);
            assertEquals(SIZE, result.size());
        }
    }
}