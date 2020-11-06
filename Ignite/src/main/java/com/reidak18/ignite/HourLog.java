package com.reidak18.ignite;

public class HourLog
{
    public String Hour = "";
    public int Count0 = 0; // emerg, panic
    public int Count1 = 0; // alert
    public int Count2 = 0; // crit
    public int Count3 = 0; // err
    public int Count4 = 0; // warn
    public int Count5 = 0; // notice
    public int Count6 = 0; // info
    public int Count7 = 0; // debug

    public HourLog(String hour)
    {
        Hour = hour;
    }

    public String Print()
    {
        return "'" + Hour + "' " + Count0 + " " + Count1 + " " + Count2 + " " + Count3 + " " + Count4 + " " +
                Count5 + " " + Count6 + " " + Count7;
    }

}
