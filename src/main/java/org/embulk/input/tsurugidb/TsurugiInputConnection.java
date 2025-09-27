package org.embulk.input.tsurugidb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.embulk.input.tsurugidb.TsurugiInputPlugin.PluginTask;
import org.embulk.input.tsurugidb.executor.TsurugiKvsExecutor;
import org.embulk.input.tsurugidb.executor.TsurugiSqlExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcInputConnection.java
public class TsurugiInputConnection implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiInputConnection.class);

    public static TsurugiInputConnection newConnection(PluginTask task) throws ServerException {
        String endpoint = task.getEndpoint();
        var credential = createCredential(task);
        logger.debug("endpoint={}, credential={}", endpoint, credential);
        int connectTimeout = task.getConnectTimeout();
        int closeTimeout = task.getSocketTimeout();

        Session session;
        try {
            var builder = SessionBuilder.connect(endpoint) //
                    .withCredential(credential) //
                    .withApplicationName("embulk-input-tsurugidb") //
                    .withLabel(task.getConnectionLabel());
            task.getSessionKeepAlive().ifPresent(b -> builder.withKeepAlive(b));
            session = builder.create(connectTimeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        session.setCloseTimeout(new Timeout(closeTimeout, TimeUnit.SECONDS, Policy.WARN));

        return new TsurugiInputConnection(task, session);
    }

    private static Credential createCredential(PluginTask task) {
        Optional<String> user = task.getUser();
        if (user.isPresent()) {
            String password = task.getPassword().orElse(null);
            return new UsernamePasswordCredential(user.get(), password);
        }

        Optional<String> token = task.getAuthToken();
        if (token.isPresent()) {
            return new RememberMeCredential(token.get());
        }

        Optional<String> credentials = task.getCredentials();
        if (credentials.isPresent()) {
            try {
                return FileCredential.load(Path.of(credentials.get()));
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

        return NullCredential.INSTANCE;
    }

    private final PluginTask task;
    private final Session session;
    private TsurugiSqlExecutor sqlExecutor;
    private TsurugiKvsExecutor kvsExecutor;

    public TsurugiInputConnection(PluginTask task, Session session) {
        this.task = task;
        this.session = session;
    }

    public PluginTask getTask() {
        return this.task;
    }

    public synchronized TsurugiSqlExecutor getSqlExecutor() {
        if (sqlExecutor == null) {
            sqlExecutor = TsurugiSqlExecutor.create(task, session);
        }
        return sqlExecutor;
    }

    public synchronized TsurugiKvsExecutor getKvsExecutor() {
        if (kvsExecutor == null) {
            kvsExecutor = TsurugiKvsExecutor.create(this, session);
        }
        return kvsExecutor;
    }

    public boolean tableExists(String tableName) throws ServerException {
        return findTableMetadata(tableName).isPresent();
    }

    public Optional<TableMetadata> findTableMetadata(String tableName) throws ServerException {
        return getSqlExecutor().findTableMetadata(tableName);
    }

    public List<String> getPrimaryKeys(String tableName) throws ServerException {
        return getSqlExecutor().getPrimaryKeys(tableName);
    }

    @Override
    public void close() throws ServerException {
        try (session) {
            try (var s = sqlExecutor; var k = kvsExecutor) {
                // close only
            }

            var shutdownType = task.getSessionShutdownType().toShutdownType();
            if (shutdownType != null) {
                int timeout = task.getSessionShutdownTimeout();
                session.shutdown(shutdownType).await(timeout, TimeUnit.SECONDS);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
