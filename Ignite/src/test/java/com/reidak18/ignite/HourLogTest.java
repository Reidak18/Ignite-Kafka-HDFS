package com.reidak18.ignite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HourLogTest
{
    HourLog log;

    @Test
    public void ConstructorTest()
    {
        log = new HourLog("Nov 1 13");
        assertEquals("Nov 1 13", log.Hour);
    }

    @Test
    public void PrintTest1()
    {
        log = new HourLog("Nov 1 13");
        assertEquals("'Nov 1 13' 0 0 0 0 0 0 0 0", log.Print());
    }

    @Test
    public void PrintTest2()
    {
        log = new HourLog("Nov 1 13");
        log.Count0 = 1;
        log.Count1 = 1;
        log.Count2 = 1;
        log.Count3 = 1;
        log.Count4 = 1;
        log.Count5 = 1;
        log.Count6 = 1;
        log.Count7 = 1;
        assertEquals("'Nov 1 13' 1 1 1 1 1 1 1 1", log.Print());
    }
}
