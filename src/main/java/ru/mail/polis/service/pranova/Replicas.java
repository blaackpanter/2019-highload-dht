package ru.mail.polis.service.pranova;

import org.jetbrains.annotations.NotNull;

public class Replicas {
    private final int ack;
    private final int from;
    private static Replicas quorum;

    private Replicas(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public static Replicas quorum(final int count) {
        if (quorum == null) {
            quorum = new Replicas(count / 2 + 1, count);
        }
        return quorum;
    }

    public static Replicas parser(@NotNull final String replicas) {
        final String[] params = replicas.split("/");
        return new Replicas(Integer.parseInt(params[0]), Integer.parseInt(params[1]));
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }
}
