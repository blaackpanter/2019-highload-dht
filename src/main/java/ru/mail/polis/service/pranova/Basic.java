package ru.mail.polis.service.pranova;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Basic implements Topology<String> {
    private final String[] servers;
    private final String me;

    /**
     * Basic realization of cluster topology.
     *
     * @param topology all url in cluster.
     * @param me       url current server.
     */
    public Basic(@NotNull final Set<String> topology, @NotNull final String me) {
        servers = new String[topology.size()];
        topology.toArray(servers);
        Arrays.sort(servers);
        this.me = me;
    }

    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        final int number = key.hashCode() & Integer.MAX_VALUE % servers.length;
        return servers[number];
    }

    @Override
    public Set<String> primaryFor(@NotNull ByteBuffer key, @NotNull Replicas replicas) {
        final Set<String> result = new HashSet<>();
        int startIndex = key.hashCode() & Integer.MAX_VALUE % servers.length;
        while (result.size() < replicas.getFrom()) {
            result.add(servers[startIndex]);
            startIndex++;
            if (startIndex == servers.length) {
                startIndex = 0;
            }
        }
        return result;
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
