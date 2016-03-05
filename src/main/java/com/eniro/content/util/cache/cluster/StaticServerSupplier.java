package com.eniro.content.util.cache.cluster;

import com.google.common.collect.Sets;

import java.net.URI;
import java.util.Set;
import java.util.function.Supplier;

public class StaticServerSupplier implements Supplier<Set<URI>> {
    private final Set<URI> servers;


    public StaticServerSupplier(URI... servers) {
        this(Sets.newHashSet(servers));
    }

    public StaticServerSupplier(Set<URI> servers) {
        this.servers = servers;
    }

    public Set<URI> get() {
        return servers;
    }
}
