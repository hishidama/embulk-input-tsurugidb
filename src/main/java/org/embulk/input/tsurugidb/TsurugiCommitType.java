package org.embulk.input.tsurugidb;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;
import com.tsurugidb.tsubakuro.kvs.CommitType;

public enum TsurugiCommitType {
    DEFAULT(CommitStatus.COMMIT_STATUS_UNSPECIFIED, CommitType.UNSPECIFIED), //
    ACCEPTED(CommitStatus.ACCEPTED, CommitType.ACCEPTED), //
    AVAILABLE(CommitStatus.AVAILABLE, CommitType.AVAILABLE), //
    STORED(CommitStatus.STORED, CommitType.STORED), //
    PROPAGATED(CommitStatus.PROPAGATED, CommitType.PROPAGATED);

    private final CommitStatus commitStatus;
    private final CommitType kvsCommitType;

    private TsurugiCommitType(CommitStatus status, CommitType kvsCommitType) {
        this.commitStatus = status;
        this.kvsCommitType = kvsCommitType;
    }

    public CommitStatus toCommitStatus() {
        return this.commitStatus;
    }

    public CommitType toKvsCommitType() {
        return this.kvsCommitType;
    }

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static TsurugiCommitType fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (Exception e) {
            var ce = new ConfigException(MessageFormat.format("Unknown insert_method ''{0}''. Supported modes are {1}", //
                    value, Arrays.stream(values()).map(TsurugiCommitType::toString).collect(Collectors.joining(", "))));
            ce.addSuppressed(e);
            throw ce;
        }
    }
}
