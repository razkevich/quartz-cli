package org.razkevich.quartz;

import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Utility for printing Quartz objects in a readable format
 */
public class QuartzPrinter {
    
    /**
     * Print scheduler info
     */
    public static void printSchedulerInfo(Scheduler scheduler) throws SchedulerException {
        System.out.println("Scheduler Information:");
        System.out.println("---------------------");
        System.out.println("Name: " + scheduler.getSchedulerName());
        System.out.println("Instance ID: " + scheduler.getSchedulerInstanceId());
        System.out.println("Version: " + scheduler.getMetaData().getVersion());
        System.out.println("Running Since: " + DateTimeUtils.formatDate(scheduler.getMetaData().getRunningSince()));
        String state = scheduler.isInStandbyMode() ? "PAUSED" : 
                     (scheduler.isStarted() ? "STARTED" : "STOPPED");
        System.out.println("State: " + state);
        System.out.println("Thread Pool Size: " + scheduler.getMetaData().getThreadPoolSize());
        System.out.println("Job Store: " + scheduler.getMetaData().getJobStoreClass().getSimpleName());
        System.out.println("Clustered: " + scheduler.getMetaData().isJobStoreClustered());
        System.out.println("Persistent: " + scheduler.getMetaData().isJobStoreSupportsPersistence());
        System.out.println();
    }
}