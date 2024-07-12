package org.embulk.input.tsurugidb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.AssumptionViolatedException;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.exception.CcException;
import com.tsurugidb.tsubakuro.sql.exception.TargetNotFoundException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class TsurugiTestTool {

    public static final String ENDPOINT;
    static {
        String endpoint = System.getProperty("endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            endpoint = "tcp://localhost:12345";
        } else if (endpoint.equalsIgnoreCase("skip")) {
            endpoint = null;
        }
        ENDPOINT = endpoint;
    }

    private static final long TIMEOUT = 10;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;

    private static Session staticSession;
    private static SqlClient staticSqlClient;

    @AfterClass
    public static void afterAll() {
        try (var c1 = staticSession; var c2 = staticSqlClient) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            staticSession = null;
            staticSqlClient = null;
        }
    }

    protected static Session getSession() {
        if (staticSession == null) {
            if (ENDPOINT == null) {
                throw new AssumptionViolatedException("endpoint not specified. skipped test");
            }
            try {
                staticSession = SessionBuilder.connect(ENDPOINT) //
                        .withApplicationName("embulk-input-tsurugidb.test") //
                        .withLabel("embulk-input-tsurugidb.test.static-session") //
                        .create(TIMEOUT, TIMEOUT_UNIT);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            } catch (ServerException e) {
                throw new ServerRuntimeException(e);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        return staticSession;
    }

    protected static SqlClient getSqlClient() {
        if (staticSqlClient == null) {
            var session = getSession();
            staticSqlClient = SqlClient.attach(session);
        }
        return staticSqlClient;
    }

    protected static PreparedStatement prepare(SqlClient sqlClient, String sql, List<Placeholder> placeholders) throws IOException, ServerException, InterruptedException, TimeoutException {
        return sqlClient.prepare(sql, placeholders).await(TIMEOUT, TIMEOUT_UNIT);
    }

    protected static ResultSet executeQuery(Transaction transaction, String sql) throws IOException, ServerException, InterruptedException, TimeoutException {
        return transaction.executeQuery(sql).await(TIMEOUT, TIMEOUT_UNIT);
    }

    protected static boolean existsTable(String tableName) {
        var sqlClient = getSqlClient();
        try {
            sqlClient.getTableMetadata(tableName).await(TIMEOUT, TIMEOUT_UNIT);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TargetNotFoundException e) {
            return false;
        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    protected static void dropTable(String tableName) {
        if (!existsTable(tableName)) {
            return;
        }

        String sql = "drop table " + tableName;
        executeDdl(sql);
    }

    protected static void createTable(String sql) {
        executeDdl(sql);
    }

    protected static void executeDdl(String sql) {
        for (int i = 0; i < 3; i++) {
            boolean success = executeOcc((sqlClient, transaction) -> {
                transaction.executeStatement(sql);
            });
            if (success) {
                return;
            }
        }
        throw new RuntimeException("retry over. sql=" + sql);
    }

    @FunctionalInterface
    public interface TsurugiAction {
        public void execute(SqlClient sqlClient, Transaction transaction) throws IOException, ServerException, InterruptedException, TimeoutException;
    }

    protected static boolean executeOcc(TsurugiAction action) {
        return execute(TransactionType.SHORT, action);
    }

    protected static boolean executeRtx(TsurugiAction action) {
        return execute(TransactionType.READ_ONLY, action);
    }

    protected static boolean execute(TransactionType txType, TsurugiAction action) {
        var sqlClient = getSqlClient();
        var option = TransactionOption.newBuilder().setType(txType).build();
        try (var transaction = createTransaction(sqlClient, option)) {
            action.execute(sqlClient, transaction);
            transaction.commit(CommitStatus.COMMIT_STATUS_UNSPECIFIED);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (CcException e) {
            return false;
        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    protected static Transaction createTransaction(SqlClient sqlClient, TransactionOption option) {
        try {
            return sqlClient.createTransaction(option).await(TIMEOUT, TIMEOUT_UNIT);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    protected static Placeholder placeholder(String name, AtomType type) {
        return Placeholders.of(name, type);
    }

    protected static Parameter parameter(String name, int value) {
        return Parameters.of(name, value);
    }

    protected static Parameter parameter(String name, long value) {
        return Parameters.of(name, value);
    }

    protected static Parameter parameter(String name, double value) {
        return Parameters.of(name, value);
    }

    protected static Parameter parameter(String name, String value) {
        return Parameters.of(name, value);
    }

    @FunctionalInterface
    public interface FutureSupplier<T> {
        public FutureResponse<T> get() throws IOException, ServerException, InterruptedException, TimeoutException;
    }
}
