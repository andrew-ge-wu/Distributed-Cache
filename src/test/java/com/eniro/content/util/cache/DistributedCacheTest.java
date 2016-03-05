package com.eniro.content.util.cache;

import com.google.common.cache.CacheBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

/**
 * Project: <social-hub-common>
 * Created by andrew on 05/03/16.
 */
public class DistributedCacheTest {
    DistributedCache<String> instance1;
    DistributedCache<String> instance2;
    DistributedCache<String> instance3;

    public DistributedCacheTest() throws URISyntaxException, InterruptedException {
        instance3 = new DistributedCache<>(CacheBuilder.newBuilder().build(), new URI("tcp://*:2000"), false, new URI("tcp://127.0.0.1:2000"), new URI("tcp://127.0.0.1:1998"), new URI("tcp://127.0.0.1:1999"));
        instance2 = new DistributedCache<>(CacheBuilder.newBuilder().build(), new URI("tcp://*:1999"), false, new URI("tcp://127.0.0.1:2000"), new URI("tcp://127.0.0.1:1998"), new URI("tcp://127.0.0.1:1999"));
        instance1 = new DistributedCache<>(CacheBuilder.newBuilder().build(), new URI("tcp://*:1998"), false, new URI("tcp://127.0.0.1:2000"), new URI("tcp://127.0.0.1:1998"), new URI("tcp://127.0.0.1:1999"));
        Thread.sleep(2000);
    }


    @Test
    public void testInvalidation() throws Exception {
        Thread.sleep(2000);
        instance2.put("blah", "value");
        instance1.put("blah", "value");
        Assert.assertNotNull(
                instance1.getIfPresent("blah"));
        instance1.invalidate("blah");
        Thread.sleep(10);

        Assert.assertNull(
                instance1.getIfPresent("blah"));
        Assert.assertNull(
                instance2.getIfPresent("blah"));
        Assert.assertNull(
                instance3.getIfPresent("blah"));
    }

    @Test
    public void testSpeed() throws Exception {
        int size = 1000;
        long start=System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            instance1.invalidate("blah");

        }
        long took = System.currentTimeMillis() - start;
        System.out.print("Finished in "+took+"ms mps:"+(1000/took)*size);
    }
}