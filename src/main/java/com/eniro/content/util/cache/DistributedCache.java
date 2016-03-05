package com.eniro.content.util.cache;

import com.eniro.content.util.cache.cluster.StaticServerSupplier;
import com.google.common.cache.Cache;
import com.google.common.cache.ForwardingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Project: <social-hub>
 * Created by andrew on 04/03/16.
 */
public class DistributedCache<V> extends ForwardingCache<String, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedCache.class);
    private final URI localBind;
    private Collection<URI> subscriberList;

    private enum Method {INV}

    public enum StatsKey {MsgSend, MsgReceive, AvgLatency, CacheStats}

    private static final String SAP = "##";
    private final Cache<String, V> delegate;
    private final Supplier<Set<URI>> serverSupplier;
    private ZMQ.Socket publisher;
    private List<Thread> subscribers;
    private AtomicInteger receiveCounter = new AtomicInteger(0);
    private AtomicInteger sendCounter = new AtomicInteger(0);
    private AtomicLong latency = new AtomicLong(0);

    public DistributedCache(Cache<String, V> delegate, URI localBind, boolean excludeLocal, URI... servers) {
        this(delegate, localBind, excludeLocal, new StaticServerSupplier(servers));
    }

    public DistributedCache(Cache<String, V> delegate, URI localBind, boolean excludeLocal, Set<URI> servers) {
        this(delegate, localBind, excludeLocal, new StaticServerSupplier(servers));
    }

    public DistributedCache(Cache<String, V> delegate, URI localBind, boolean excludeLocal, Supplier<Set<URI>> serverSupplier) {
        this.delegate = delegate;
        this.serverSupplier = serverSupplier;
        this.localBind = localBind;
        updateSubscriber(excludeLocal);
    }

    public void init() {
        if (publisher == null) {
            publisher = createPublisher(localBind.toString());
        }
        subscribers = subscriberList.stream().map(host -> new Thread(() -> {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
            subscriber.connect(host.toString());
            subscriber.subscribe(new byte[]{});
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    LOGGER.debug("Waiting message from socket");
                    String msg = subscriber.recvStr(0).trim();
                    receive(new Msg(msg));
                }
            } catch (RuntimeException e) {
                if (e.getCause() != null && e.getCause() instanceof ClosedByInterruptException) {
                    LOGGER.warn(String.format("Subscription to host=%s terminated by interrupt", host));
                } else {
                    LOGGER.warn(String.format("Subscription to host=%s terminated for unknown reason", host), e);
                }
            } finally {
                subscriber.close();
            }
        }, "Subscriber-" + host.getHost())).collect(Collectors.toList());
        subscribers.stream().forEach(Thread::start);
    }

    public void updateSubscriber(boolean excludeLocal) {
        Set<URI> subs = serverSupplier.get();
        if (excludeLocal) {
            subs = subs.stream().filter(uri -> {
                try {
                    InetAddress address = InetAddress.getByName(uri.getHost());
                    return !address.isAnyLocalAddress() && !address.isLoopbackAddress();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    return false;
                }
            }).collect(Collectors.toSet());
        }
        if (this.subscriberList == null || subs.size() != this.subscriberList.size() || subs.containsAll(this.subscriberList)) {
            shutdown();
            this.subscriberList = subs;
            init();
        }
    }

    private static ZMQ.Socket createPublisher(String bindHost) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket publisher = context.socket(ZMQ.PUB);
        publisher.bind(bindHost);
        return publisher;
    }

    private void receive(Msg msg) {
        LOGGER.debug("Processing message:{}", msg.toString());
        switch (msg.method) {
            case INV:
                if (msg.payload.length > 0) {
                    if (msg.payload.length > 1) {
                        silentInvalidateAll(Lists.newArrayList(msg.payload));
                    } else {
                        silentInvalidate(msg.payload[0]);
                    }
                }
                break;
            default:
                break;
        }
        receiveCounter.incrementAndGet();
        latency.addAndGet(System.currentTimeMillis() - msg.timestamp);
    }

    private void send(Msg msg) {
        if (subscribers.size() > 0) {
            String toSend = msg.toString();
            LOGGER.debug("Sending:{}", toSend);
            publisher.send(toSend);
            sendCounter.incrementAndGet();
        }
    }


    @Override
    public void invalidate(Object key) {
        super.invalidate(key);
        send(new Msg(Method.INV, key.toString()));
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        super.invalidateAll(keys);
        send(new Msg(Method.INV, Iterables.toArray(keys, Object.class)));
    }

    private void silentInvalidate(Object key) {
        super.invalidate(key);
    }


    private void silentInvalidateAll(Iterable<?> keys) {
        super.invalidateAll(keys);
    }


    @Override
    protected Cache<String, V> delegate() {
        return delegate;
    }


    public void shutdown() {
        if (publisher != null) {
            publisher.close();
            publisher = null;
        }
        if (subscribers != null) {
            subscribers.stream().forEach(Thread::interrupt);
            subscribers.clear();
        }
    }

    public void printStats() {
        LOGGER.info("Total messages sent:{}", sendCounter.toString());
        LOGGER.info("Total messages received:{}", receiveCounter.toString());
        LOGGER.info("Average message latency:{}ms", receiveCounter.intValue() > 0 ? latency.longValue() / receiveCounter.intValue() : 0);
        LOGGER.info(stats().toString());
    }

    public Map<StatsKey, Object> getStats() {
        Map<StatsKey, Object> toReturn = new HashMap<>();
        toReturn.put(StatsKey.MsgSend, sendCounter.intValue());
        toReturn.put(StatsKey.MsgReceive, receiveCounter.intValue());
        toReturn.put(StatsKey.AvgLatency, receiveCounter.intValue() > 0 ? latency.longValue() / receiveCounter.intValue() : 0);
        toReturn.put(StatsKey.CacheStats, stats());
        return toReturn;
    }

    private class Msg {
        private final Object[] payload;
        private final Method method;
        private final long timestamp;

        public Msg(Method method, Collection<Object> payload) {
            this(method, payload.toArray(new Object[payload.size()]));
        }

        public Msg(Method method, Object... payload) {
            this.method = method;
            this.payload = payload;
            this.timestamp = System.currentTimeMillis();
        }

        public Msg(String input) {
            String[] fragments = input.split(SAP);
            if (fragments.length == 3) {
                this.method = Method.valueOf(fragments[0]);
                this.payload = fragments[1].split(",");
                this.timestamp = Long.valueOf(fragments[2]);
            } else {
                throw new IllegalArgumentException("Can not parse input:" + input);
            }
        }


        @Override
        public String toString() {
            return method.name() +
                    SAP + String.join(",", Arrays.copyOf(payload, payload.length, String[].class)) +
                    SAP + timestamp;
        }
    }

}

