package ru.mail.polis.service.pranova;

import com.google.common.base.Splitter;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class Replicas {
    private final int ack;
    private final int from;

    private Replicas(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public static Replicas quorum(final int count) {
        return new Replicas(count / 2 + 1, count);
    }

    public static Replicas parser(@NotNull final String replicas) {
        final List<String> params = Splitter.on('/').splitToList(replicas);
        return new Replicas(Integer.parseInt(params.get(0)), Integer.parseInt(params.get(1)));
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    @Override
    public String toString() {
        return ack + "/" + from;
    }
}
