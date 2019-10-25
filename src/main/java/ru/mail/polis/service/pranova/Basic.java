package ru.mail.polis.service.pranova;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class Basic implements Topology<String> {
    private final String[] servers;
    private final String me;

    public Basic(@NotNull final Set<String> topology, @NotNull final String me) {
        servers = new String[topology.size()];
        topology.toArray(servers);
        Arrays.sort(servers);
        this.me = me;
    }

    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        int number = Math.abs(key.hashCode()) % servers.length;
        return servers[number];
    }

    @Override
    public boolean isMe(@NotNull final String node) {
        return node.equals(me);
    }

    @Override
    public Set<String> all() {
        return Set.of(servers);
    }
}
