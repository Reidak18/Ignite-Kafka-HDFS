package com.reidak18.ignite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.compute.ComputeJobResult;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MapReduceTest
{
    IgniteMapperReducer mapperReducer;

    @Before
    public void setUp() throws URISyntaxException, IOException
    {
        mapperReducer = new IgniteMapperReducer();
    }

    @Test
    public void ParceTest()
    {
        HourLog hourLog;
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 emerg");
        assertEquals(1, hourLog.Count0);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 panic");
        assertEquals(1, hourLog.Count0);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 alert");
        assertEquals(1, hourLog.Count1);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 crit");
        assertEquals(1, hourLog.Count2);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 err");
        assertEquals(1, hourLog.Count3);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 warn");
        assertEquals(1, hourLog.Count4);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 notice");
        assertEquals(1, hourLog.Count5);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 info");
        assertEquals(1, hourLog.Count6);
        hourLog = mapperReducer.Parce("Nov  1 08:11:37 debug");
        assertEquals(1, hourLog.Count7);
    }
}
