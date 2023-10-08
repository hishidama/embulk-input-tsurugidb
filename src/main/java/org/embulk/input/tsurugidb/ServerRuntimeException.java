package org.embulk.input.tsurugidb;

import com.tsurugidb.tsubakuro.exception.ServerException;

@SuppressWarnings("serial")
public class ServerRuntimeException extends RuntimeException {

    public ServerRuntimeException(ServerException cause) {
        super(createMessage(cause), cause);
    }

    protected static String createMessage(ServerException e) {
        var code = e.getDiagnosticCode();
        if (code != null) {
            return code.name() + ": " + e.getMessage();
        } else {
            return e.getMessage();
        }
    }

    @Override
    public ServerException getCause() {
        return (ServerException) super.getCause();
    }
}
