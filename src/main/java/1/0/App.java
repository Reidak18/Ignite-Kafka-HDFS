package com.reidak18.ignite;
import org.apache.ignite.*;

public final class App 
{
    private App() 
    {
        
    }

    public static void main(String[] args) 
    {
        Ignition.start();
        System.out.println("Hello World!");
    }
}
