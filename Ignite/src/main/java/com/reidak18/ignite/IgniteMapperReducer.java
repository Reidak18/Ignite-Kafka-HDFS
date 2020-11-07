package com.reidak18.ignite;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

// Класс обработки данных
public class IgniteMapperReducer extends ComputeTaskAdapter<String, ArrayList>
{
    // В map создаем отдельный экземпляр каждой строки в syslog, заполняем Hour и счетчики для нее
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, String arg)
    {
        Map<ComputeJob, ClusterNode> map = new HashMap<>();

        Iterator<ClusterNode> it = nodes.iterator();

        for (final String log : arg.split("\n"))
        {
            // Если мы использовали все узлы, перезапускаем итератор.
            if (!it.hasNext())
                it = nodes.iterator();

            ClusterNode node = it.next();

            map.put(new ComputeJobAdapter()
            {
                @Nullable @Override
                public Object execute()
                {
                    System.out.println();
                    System.out.println(">>> Parsing '" + log + "' on this node from ignite job.");
                    HourLog hourLog = new HourLog(log);

                    if (log.contains("emerg") || log.contains("panic")) hourLog.Count0++;
                    else if (log.contains("alert")) hourLog.Count1++;
                    else if (log.contains("crit")) hourLog.Count2++;
                    else if (log.contains("err")) hourLog.Count3++;
                    else if (log.contains("warn")) hourLog.Count4++;
                    else if (log.contains("notice")) hourLog.Count5++;
                    else if (log.contains("info")) hourLog.Count6++;
                    else if (log.contains("debug")) hourLog.Count7++;

                    return hourLog;
                }
            }, node);
        }

        return map;
    }

    // В reduce суммируем строки для одного часа и получаем суммарные значения счетчиков
    @Nullable @Override
    public ArrayList reduce(List<ComputeJobResult> results)
    {
        List logs = new ArrayList();

        Map<String, List<ComputeJobResult>> Grouped =
                results.stream().collect(
                        Collectors.groupingBy(w -> w.<HourLog>getData().Hour.substring(0, 9)));

        for (String key:Grouped.keySet())
        {
            HourLog log = new HourLog(key);

            for (ComputeJobResult res: Grouped.get(key))
            {
                log.Count0 += res.<HourLog>getData().Count0;
                log.Count1 += res.<HourLog>getData().Count1;
                log.Count2 += res.<HourLog>getData().Count2;
                log.Count3 += res.<HourLog>getData().Count3;
                log.Count4 += res.<HourLog>getData().Count4;
                log.Count5 += res.<HourLog>getData().Count5;
                log.Count6 += res.<HourLog>getData().Count6;
                log.Count7 += res.<HourLog>getData().Count7;
            }

            logs.add(log);
        }

        return (ArrayList) logs;
    }

}
