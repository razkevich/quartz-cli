package org.razkevich.quartz;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import java.util.Date;

import java.util.concurrent.Callable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import javax.sql.DataSource;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Command-line interface for Quartz Scheduler
 */
@Command(name = "quartz-cli",
         description = "Quartz Database CLI Tool",
         mixinStandardHelpOptions = true,
         version = "1.0")
public class QuartzCli implements Callable<Integer> {
    
    @Option(names = {"-u", "--url"},
            description = "JDBC URL for Quartz database", 
            required = true)
    private String jdbcUrl;
    
    @Option(names = {"-U", "--user"}, 
            description = "Database username", 
            required = true)
    private String username;
    
    @Option(names = {"-P", "--password"}, 
            description = "Database password", 
            required = true)
    private String password;
    
    @Option(names = {"-d", "--driver"}, 
            description = "JDBC driver class name",
            defaultValue = "org.postgresql.Driver")
    private String driver;
    
    @Option(names = {"-s", "--schema"}, 
            description = "Database schema containing Quartz tables")
    private String schema;
    
    @Option(names = {"-p", "--prefix"}, 
            description = "Quartz table prefix",
            defaultValue = "qrtz_")
    private String tablePrefix;
    
    @Option(names = {"-c", "--command"}, 
            description = "Command to execute (list-jobs, list-triggers, list-running, list-paused, list-schedulers, clear, view-job, view-trigger, delete-job, delete-trigger)",
            required = true)
    private String command;
    
    @Option(names = {"-g", "--group"}, 
            description = "Group filter for jobs or triggers")
    private String group;
    
    @Option(names = {"-n", "--name"}, 
            description = "Name filter for jobs or triggers")
    private String name;
    
    @Option(names = {"-t", "--trigger"}, 
            description = "Trigger name filter")
    private String triggerName;
    
    @Option(names = {"-f", "--force"}, 
            description = "Force clear operation without confirmation")
    private boolean forceClear;
    
    @Option(names = {"-v", "--verbose"}, 
            description = "Enable verbose output")
    private boolean verbose;
    
    @Option(names = {"-S", "--scheduler"}, 
            description = "Scheduler name filter")
    private String schedulerName;
    
    @Option(names = {"--json"}, 
            description = "Output in JSON format")
    private boolean jsonOutput;
    
    @Override
    public Integer call() throws Exception {
        try {
            switch (command.toLowerCase()) {
                case "help":
                    showHelp();
                    break;
                case "list-jobs":
                    listJobsWithSQL(group, name);
                    break;
                case "list-triggers":
                    listTriggersWithSQL(group, name);
                    break;
                case "list-running":
                    listRunningJobs();
                    break;
                case "list-paused":
                    listPausedTriggerGroups();
                    break;
                case "list-schedulers":
                    listSchedulers();
                    break;
                case "clear":
                    clearTables();
                    break;
                case "view-job":
                    if (group == null && name == null) {
                        System.out.println("Error: At least one of group or name must be specified for view-job");
                        return 1;
                    }
                    viewJob(group, name);
                    break;
                case "view-trigger":
                    if (group == null && name == null) {
                        System.out.println("Error: At least one of group or name must be specified for view-trigger");
                        return 1;
                    }
                    viewTrigger(group, name);
                    break;
                case "delete-job":
                    if (group == null && name == null) {
                        System.out.println("Error: At least one of group or name must be specified for delete-job");
                        return 1;
                    }
                    deleteJob(group, name);
                    break;
                case "delete-trigger":
                    if (group == null && name == null) {
                        System.out.println("Error: At least one of group or name must be specified for delete-trigger");
                        return 1;
                    }
                    deleteTrigger(group, name);
                    break;
                case "webapp":
                    startWebApp();
                    break;
                default:
                    System.out.println("Unknown command: " + command);
                    showHelp();
                    return 1;
            }
            return 0;
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            return 1;
        }
    }
    
    private void showHelp() {
        System.out.println("Quartz CLI - Available Commands:");
        System.out.println();
        
        Table table = Table.create("Commands")
            .addColumns(
                StringColumn.create("Command"),
                StringColumn.create("Description"),
                StringColumn.create("Required Parameters")
            );
        
        table.stringColumn("Command").append("help");
        table.stringColumn("Description").append("Show this help message");
        table.stringColumn("Required Parameters").append("None");
        
        table.stringColumn("Command").append("list-jobs");
        table.stringColumn("Description").append("List all jobs with optional group/name filters");
        table.stringColumn("Required Parameters").append("None (optional: --group, --name)");
        
        table.stringColumn("Command").append("list-triggers");
        table.stringColumn("Description").append("List all triggers with optional group/name filters");
        table.stringColumn("Required Parameters").append("None (optional: --group, --name)");
        
        table.stringColumn("Command").append("list-running");
        table.stringColumn("Description").append("List currently running jobs");
        table.stringColumn("Required Parameters").append("None");
        
        table.stringColumn("Command").append("list-paused");
        table.stringColumn("Description").append("List paused trigger groups");
        table.stringColumn("Required Parameters").append("None");
        
        table.stringColumn("Command").append("list-schedulers");
        table.stringColumn("Description").append("List all schedulers");
        table.stringColumn("Required Parameters").append("None");
        
        table.stringColumn("Command").append("view-job");
        table.stringColumn("Description").append("View detailed information about a job");
        table.stringColumn("Required Parameters").append("At least one of: --group, --name");
        
        table.stringColumn("Command").append("view-trigger");
        table.stringColumn("Description").append("View detailed information about a trigger");
        table.stringColumn("Required Parameters").append("At least one of: --group, --name");
        
        table.stringColumn("Command").append("delete-job");
        table.stringColumn("Description").append("Delete a job and its triggers");
        table.stringColumn("Required Parameters").append("At least one of: --group, --name");
        
        table.stringColumn("Command").append("delete-trigger");
        table.stringColumn("Description").append("Delete a trigger");
        table.stringColumn("Required Parameters").append("At least one of: --group, --name");
        
        table.stringColumn("Command").append("clear");
        table.stringColumn("Description").append("Clear all Quartz tables (requires --force)");
        table.stringColumn("Required Parameters").append("--force");
        
        table.stringColumn("Command").append("webapp");
        table.stringColumn("Description").append("Start web application interface on port 8080");
        table.stringColumn("Required Parameters").append("None");
        
        System.out.println(TableFormatter.formatTable(table));
        System.out.println();
        System.out.println("Common Options:");
        System.out.println("  --group, -g    : Filter by group name (partial match)");
        System.out.println("  --name, -n     : Filter by name (partial match)");
        System.out.println("  --force, -f    : Skip confirmation prompts");
        System.out.println("  --json         : Output in JSON format");
        System.out.println("  --verbose, -v  : Enable verbose output");
        System.out.println("  --scheduler, -S: Filter by scheduler name");
    }
    
    /**
     * Start the web application interface
     */
    private void startWebApp() {
        System.out.println("Starting Quartz Web Console on port 8080...");
        System.out.println("Database URL: " + jdbcUrl);
        
        // Set system properties for the web application to use
        System.setProperty("quartz.jdbc.url", jdbcUrl);
        System.setProperty("quartz.jdbc.username", username);
        System.setProperty("quartz.jdbc.password", password);
        System.setProperty("quartz.jdbc.driver", driver);
        if (schema != null) {
            System.setProperty("quartz.jdbc.schema", schema);
        }
        System.setProperty("quartz.table.prefix", tablePrefix);
        
        // Create a latch to keep the application running
        final CountDownLatch latch = new CountDownLatch(1);
        
        // Start the Spring Boot application in a separate thread
        try {
            // Create and configure the Spring Boot application
            org.springframework.boot.SpringApplication app = 
                new org.springframework.boot.SpringApplication(org.razkevich.quartz.QuartzWebApp.class);
            
            // Start the application and get the context
            ConfigurableApplicationContext context = app.run(new String[]{});
            
            // Add a shutdown hook to release the latch when the application is stopped
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    System.out.println("Shutting down Quartz Web Console...");
                    latch.countDown();
                }
            });
            
            System.out.println("Quartz Web Console is running. Press Ctrl+C to stop.");
            
            // Wait for the latch to be released (which will happen when the JVM is shutting down)
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            System.out.println("Web application has been stopped.");
            
        } catch (Exception e) {
            System.err.println("Error starting web application: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    private void listJobsWithSQL(String groupFilter, String nameFilter) throws Exception {
        if (verbose) {
            System.out.println("Using SQL to list jobs...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
            
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
            
            List<Map<String, Object>> jobs = quartzDataService.listJobs(groupFilter, nameFilter);
            
            if (jsonOutput) {
                System.out.println("[");
                boolean first = true;
                for (Map<String, Object> job : jobs) {
                    if (!first) {
                        System.out.println(",");
                    }
                    first = false;
                    System.out.println("  {");
                    System.out.println("    \"group\": \"" + job.get("group") + "\",");
                    System.out.println("    \"name\": \"" + job.get("name") + "\",");
                    System.out.println("    \"class\": \"" + job.get("class") + "\",");
                    System.out.println("    \"description\": \"" + job.get("description") + "\",");
                    System.out.println("    \"triggerCount\": " + job.get("triggerCount"));
                    System.out.print("  }");
                }
                System.out.println("\n]");
            } else {
                Table table = Table.create("Jobs")
                    .addColumns(
                        StringColumn.create("Group"),
                        StringColumn.create("Name"),
                        StringColumn.create("Class"),
                        StringColumn.create("Description"),
                        IntColumn.create("Triggered")
                    );
                
                boolean hasRows = false;
                for (Map<String, Object> job : jobs) {
                    hasRows = true;
                    table.stringColumn("Group").append((String) job.get("group"));
                    table.stringColumn("Name").append((String) job.get("name"));
                    table.stringColumn("Class").append((String) job.get("class"));
                    table.stringColumn("Description").append((String) job.get("description"));
                    table.intColumn("Triggered").append((Integer) job.get("triggerCount"));
                }
                
                if (hasRows) {
                    System.out.println(TableFormatter.formatTable(table));
                } else {
                    System.out.println("No jobs found.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error listing jobs with SQL: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    private void listTriggersWithSQL(String groupFilter, String nameFilter) throws Exception {
        if (verbose) {
            System.out.println("Using SQL to list triggers...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
                
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
            
            List<Map<String, Object>> triggers = quartzDataService.listTriggers(groupFilter, nameFilter);
            
            if (jsonOutput) {
                System.out.println("[");
                boolean first = true;
                for (Map<String, Object> trigger : triggers) {
                    if (!first) {
                        System.out.println(",");
                    }
                    first = false;
                    System.out.println("  {");
                    System.out.println("    \"group\": \"" + trigger.get("group") + "\",");
                    System.out.println("    \"name\": \"" + trigger.get("name") + "\",");
                    System.out.println("    \"job\": \"" + trigger.get("jobGroup") + "." + trigger.get("jobName") + "\",");
                    System.out.println("    \"type\": \"" + trigger.get("type") + "\",");
                    System.out.println("    \"nextFire\": \"" + trigger.get("nextFireTime") + "\",");
                    System.out.println("    \"previousFire\": \"" + trigger.get("prevFireTime") + "\"");
                    System.out.print("  }");
                }
                System.out.println("\n]");
            } else {
                Table table = Table.create("Triggers")
                    .addColumns(
                        StringColumn.create("Group"),
                        StringColumn.create("Name"),
                        StringColumn.create("Job"),
                        StringColumn.create("Type"),
                        StringColumn.create("Next Fire"),
                        StringColumn.create("Previous Fire")
                    );
                
                boolean hasRows = false;
                for (Map<String, Object> trigger : triggers) {
                    hasRows = true;
                    String name = (String) trigger.get("name");
                    String triggerGroup = (String) trigger.get("group");
                    String jobName = (String) trigger.get("jobName");
                    String jobGroup = (String) trigger.get("jobGroup");
                    String triggerType = (String) trigger.get("type");
                    String nextFire = (String) trigger.get("nextFireTime");
                    String prevFire = (String) trigger.get("prevFireTime");
                    
                    table.stringColumn("Group").append(triggerGroup);
                    table.stringColumn("Name").append(name);
                    table.stringColumn("Job").append(jobGroup + "." + jobName);
                    table.stringColumn("Type").append(triggerType);
                    table.stringColumn("Next Fire").append(nextFire);
                    table.stringColumn("Previous Fire").append(prevFire);
                }
                
                if (hasRows) {
                    System.out.println(TableFormatter.formatTable(table));
                } else {
                    System.out.println("No triggers found.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error listing triggers with SQL: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Format a timestamp for relative display
     * @deprecated Use DateTimeUtils.formatRelativeTime instead
     */
    @Deprecated
    private String formatRelativeTime(long timestamp) {
        return DateTimeUtils.formatRelativeTime(timestamp);
    }
    
    private void listRunningJobs() throws Exception {
        if (verbose) {
            System.out.println("Listing currently running jobs...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
                
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
                
            List<Map<String, Object>> runningJobs = quartzDataService.listRunningJobs();
            
            if (jsonOutput) {
                System.out.println("[");
                boolean first = true;
                for (Map<String, Object> job : runningJobs) {
                    if (!first) {
                        System.out.println(",");
                    }
                    first = false;
                    System.out.println("  {");
                    System.out.println("    \"job\": \"" + job.get("jobGroup") + "." + job.get("jobName") + "\",");
                    System.out.println("    \"trigger\": \"" + job.get("triggerGroup") + "." + job.get("triggerName") + "\",");
                    System.out.println("    \"firedTime\": \"" + job.get("firedTime") + "\",");
                    System.out.println("    \"state\": \"" + job.get("state") + "\"");
                    System.out.print("  }");
                }
                System.out.println("\n]");
            } else {
                Table table = Table.create("Running Jobs")
                    .addColumns(
                        StringColumn.create("Job"),
                        StringColumn.create("Trigger"),
                        StringColumn.create("Fired Time"),
                        StringColumn.create("State")
                    );
                
                boolean hasRows = false;
                for (Map<String, Object> job : runningJobs) {
                    hasRows = true;
                    table.stringColumn("Job").append(
                        (String)job.get("jobGroup") + "." + (String)job.get("jobName"));
                    table.stringColumn("Trigger").append(
                        (String)job.get("triggerGroup") + "." + (String)job.get("triggerName"));
                    table.stringColumn("Fired Time").append((String)job.get("firedTime"));
                    table.stringColumn("State").append((String)job.get("state"));
                }
                
                if (hasRows) {
                    System.out.println(TableFormatter.formatTable(table));
                } else {
                    System.out.println("No running jobs found.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error listing running jobs: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    private void listPausedTriggerGroups() throws Exception {
        if (verbose) {
            System.out.println("Listing paused trigger groups...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
                
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
                
            List<Map<String, Object>> pausedGroups = quartzDataService.listPausedTriggerGroups();
            
            if (jsonOutput) {
                System.out.println("[");
                boolean first = true;
                for (Map<String, Object> group : pausedGroups) {
                    if (!first) {
                        System.out.println(",");
                    }
                    first = false;
                    System.out.println("  {");
                    System.out.println("    \"group\": \"" + group.get("triggerGroup") + "\"");
                    System.out.print("  }");
                }
                System.out.println("\n]");
            } else {
                Table table = Table.create("Paused Trigger Groups")
                    .addColumns(
                        StringColumn.create("Group")
                    );
                
                boolean hasRows = false;
                for (Map<String, Object> group : pausedGroups) {
                    hasRows = true;
                    table.stringColumn("Group").append((String)group.get("triggerGroup"));
                }
                
                if (hasRows) {
                    System.out.println(TableFormatter.formatTable(table));
                } else {
                    System.out.println("No paused trigger groups found.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error listing paused trigger groups: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    private void listSchedulers() throws Exception {
        if (verbose) {
            System.out.println("Listing active schedulers...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
                
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
                
            List<Map<String, Object>> schedulers = quartzDataService.listSchedulers();
            schedulers.sort((s1, s2) -> ((Date) s2.get("lastCheckinTime")).compareTo((Date) s1.get("lastCheckinTime")));
            if (jsonOutput) {
                System.out.println("[");
                boolean first = true;
                for (Map<String, Object> scheduler : schedulers) {
                    if (!first) {
                        System.out.println(",");
                    }
                    first = false;
                    System.out.println("  {");
                    System.out.println("    \"instanceName\": \"" + scheduler.get("instanceName") + "\",");
                    System.out.println("    \"lastCheckin\": \"" + scheduler.get("lastCheckinTime") + "\",");
                    System.out.println("    \"checkinInterval\": " + scheduler.get("checkinInterval"));
                    System.out.print("  }");
                }
                System.out.println("\n]");
            } else {
                Table table = Table.create("Active Schedulers")
                    .addColumns(
                        StringColumn.create("Instance Name"),
                        StringColumn.create("Last Checkin"),
                        LongColumn.create("Checkin Interval (ms)")
                    );
                
                boolean hasRows = false;
                for (Map<String, Object> scheduler : schedulers) {
                    hasRows = true;
                    String instanceName = (String) scheduler.get("instanceName");
                    String lastCheckin = (String) scheduler.get("lastCheckinTime");
                    long interval = (Long) scheduler.get("checkinInterval");
                    
                    table.stringColumn("Instance Name").append(instanceName);
                    table.stringColumn("Last Checkin").append(lastCheckin);
                    table.longColumn("Checkin Interval (ms)").append(interval);
                }
                
                if (hasRows) {
                    System.out.println(TableFormatter.formatTable(table));
                } else {
                    System.out.println("No active schedulers found.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error listing schedulers: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    private void clearTables() throws Exception {
        if (!forceClear) {
            System.out.println("\nWARNING: This will delete ALL data from Quartz tables!");
            System.out.println("This operation cannot be undone.");
            System.out.print("Are you sure you want to continue? (y/N): ");
            
            Scanner scanner = new Scanner(System.in);
            String response = scanner.nextLine().trim().toLowerCase();
            if (!response.equals("y")) {
                System.out.println("Operation cancelled.");
                return;
            }
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
                
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
                
            quartzDataService.clearTables();
            
                System.out.println("\nAll tables cleared successfully.");
        } catch (Exception e) {
            System.out.println("Error clearing tables: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            throw e;
        }
    }
    
    private void viewJob(String group, String name) throws Exception {
        if (verbose) {
            System.out.println("Viewing job details...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
                
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
                
            // First check if we have multiple results
            List<Map<String, Object>> jobs = quartzDataService.listJobs(group, name);
            
            if (jobs.isEmpty()) {
                System.out.println("No jobs found matching the criteria");
                return;
            }
            
            if (jobs.size() > 1) {
                // Multiple results found, switch to list mode
                listJobsWithSQL(group, name);
                return;
            }
            
            // Get the exact job name and group from the first result
            String exactGroup = (String) jobs.get(0).get("group");
            String exactName = (String) jobs.get(0).get("name");
            
            // Get detailed job information
            Map<String, Object> jobDetails = quartzDataService.getJobDetails(exactGroup, exactName);
            
            // Display job details
            if (jsonOutput) {
                System.out.println("{");
                System.out.println("  \"name\": \"" + jobDetails.get("name") + "\",");
                System.out.println("  \"group\": \"" + jobDetails.get("group") + "\",");
                System.out.println("  \"description\": \"" + jobDetails.get("description") + "\",");
                System.out.println("  \"jobClass\": \"" + jobDetails.get("jobClass") + "\",");
                System.out.println("  \"isDurable\": " + jobDetails.get("isDurable") + ",");
                System.out.println("  \"isNonConcurrent\": " + jobDetails.get("isNonConcurrent") + ",");
                System.out.println("  \"isUpdateData\": " + jobDetails.get("isUpdateData") + ",");
                System.out.println("  \"requestsRecovery\": " + jobDetails.get("requestsRecovery") + ",");
                System.out.println("  \"hasJobData\": " + jobDetails.get("hasJobData") + ",");
                
                // Print triggers
                System.out.println("  \"triggers\": [");
                List<Map<String, Object>> triggers = (List<Map<String, Object>>) jobDetails.get("triggers");
                boolean first = true;
                for (Map<String, Object> trigger : triggers) {
                    if (!first) {
                        System.out.println(",");
                    }
                    first = false;
                    System.out.println("    {");
                    System.out.println("      \"name\": \"" + trigger.get("name") + "\",");
                    System.out.println("      \"group\": \"" + trigger.get("group") + "\",");
                    System.out.println("      \"state\": \"" + trigger.get("state") + "\",");
                    System.out.println("      \"type\": \"" + trigger.get("type") + "\",");
                    System.out.println("      \"nextFireTime\": \"" + trigger.get("nextFireTime") + "\",");
                    System.out.println("      \"prevFireTime\": \"" + trigger.get("prevFireTime") + "\",");
                    System.out.println("      \"startTime\": \"" + trigger.get("startTime") + "\",");
                    System.out.println("      \"endTime\": \"" + trigger.get("endTime") + "\"");
                    System.out.print("    }");
                }
                System.out.println("\n  ]");
                System.out.println("}");
            } else {
                System.out.println("\nJob Details:");
                System.out.println("  Name: " + jobDetails.get("name"));
                System.out.println("  Group: " + jobDetails.get("group"));
                System.out.println("  Description: " + jobDetails.get("description"));
                System.out.println("  Job Class: " + jobDetails.get("jobClass"));
                System.out.println("  Durable: " + jobDetails.get("isDurable"));
                System.out.println("  Non-Concurrent: " + jobDetails.get("isNonConcurrent"));
                System.out.println("  Update Data: " + jobDetails.get("isUpdateData"));
                System.out.println("  Requests Recovery: " + jobDetails.get("requestsRecovery"));
                System.out.println("  Has Job Data: " + jobDetails.get("hasJobData"));
                
                // Print triggers
                List<Map<String, Object>> triggers = (List<Map<String, Object>>) jobDetails.get("triggers");
                triggers.sort((t1, t2) -> ((Date) t2.get("nextFireTime")).compareTo((Date) t1.get("nextFireTime")));
                // Print triggers
                System.out.println("\nAssociated Triggers (" + triggers.size() + "):");
                
                if (!triggers.isEmpty()) {
                    Table table = Table.create("Triggers")
                        .addColumns(
                            StringColumn.create("Name"),
                            StringColumn.create("Group"),
                            StringColumn.create("Type"),
                            StringColumn.create("State"),
                            StringColumn.create("Next Fire"),
                            StringColumn.create("Previous Fire")
                        );
                    
                    for (Map<String, Object> trigger : triggers) {
                        table.stringColumn("Name").append((String) trigger.get("name"));
                        table.stringColumn("Group").append((String) trigger.get("group"));
                        table.stringColumn("Type").append((String) trigger.get("type"));
                        table.stringColumn("State").append((String) trigger.get("state"));
                        table.stringColumn("Next Fire").append((String) trigger.get("nextFireTime"));
                        table.stringColumn("Previous Fire").append((String) trigger.get("prevFireTime"));
                    }
                    
                    System.out.println(TableFormatter.formatTable(table));
                } else {
                    System.out.println("  No triggers associated with this job");
                }
            }
        } catch (Exception e) {
            System.out.println("Error viewing job: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }

    private void viewTrigger(String group, String name) throws Exception {
        if (verbose) {
            System.out.println("Viewing trigger details...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
                
            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);
                
            // First check if we have multiple results
            List<Map<String, Object>> triggers = quartzDataService.listTriggers(group, name);
            
            if (triggers.isEmpty()) {
                System.out.println("No triggers found matching the criteria");
                return;
            }
            
            if (triggers.size() > 1) {
                // Multiple results found, switch to list mode
                listTriggersWithSQL(group, name);
                return;
            }
            
            // Get the exact trigger name and group from the first result
            String exactGroup = (String) triggers.get(0).get("group");
            String exactName = (String) triggers.get(0).get("name");
            
            // Get detailed trigger information
            Map<String, Object> triggerDetails = quartzDataService.getTriggerDetails(exactGroup, exactName);
            
            // Display trigger details
            if (jsonOutput) {
                System.out.println("{");
                System.out.println("  \"name\": \"" + triggerDetails.get("name") + "\",");
                System.out.println("  \"group\": \"" + triggerDetails.get("group") + "\",");
                System.out.println("  \"jobName\": \"" + triggerDetails.get("jobName") + "\",");
                System.out.println("  \"jobGroup\": \"" + triggerDetails.get("jobGroup") + "\",");
                System.out.println("  \"description\": \"" + triggerDetails.get("description") + "\",");
                System.out.println("  \"type\": \"" + triggerDetails.get("type") + "\",");
                System.out.println("  \"state\": \"" + triggerDetails.get("state") + "\",");
                System.out.println("  \"startTime\": \"" + triggerDetails.get("startTime") + "\",");
                System.out.println("  \"endTime\": \"" + triggerDetails.get("endTime") + "\",");
                System.out.println("  \"nextFireTime\": \"" + triggerDetails.get("nextFireTime") + "\",");
                System.out.println("  \"prevFireTime\": \"" + triggerDetails.get("prevFireTime") + "\",");
                System.out.println("  \"priority\": " + triggerDetails.get("priority") + ",");
                System.out.println("  \"misfireInstr\": " + triggerDetails.get("misfireInstr") + ",");
                
                // Add type-specific details
                if ("CRON".equals(triggerDetails.get("type"))) {
                    System.out.println("  \"cronExpression\": \"" + triggerDetails.get("cronExpression") + "\",");
                    System.out.println("  \"timeZoneId\": \"" + triggerDetails.get("timeZoneId") + "\"");
                } else if ("SIMPLE".equals(triggerDetails.get("type"))) {
                    System.out.println("  \"repeatCount\": " + triggerDetails.get("repeatCount") + ",");
                    System.out.println("  \"repeatInterval\": " + triggerDetails.get("repeatInterval") + ",");
                    System.out.println("  \"timesTriggered\": " + triggerDetails.get("timesTriggered") + "");
                }
                
                System.out.println("}");
            } else {
                System.out.println("\nTrigger Details:");
                System.out.println("  Name: " + triggerDetails.get("name"));
                System.out.println("  Group: " + triggerDetails.get("group"));
                System.out.println("  Job: " + triggerDetails.get("jobGroup") + "." + triggerDetails.get("jobName"));
                System.out.println("  Description: " + triggerDetails.get("description"));
                System.out.println("  Type: " + triggerDetails.get("type"));
                System.out.println("  State: " + triggerDetails.get("state"));
                System.out.println("  Start Time: " + triggerDetails.get("startTime"));
                System.out.println("  End Time: " + triggerDetails.get("endTime"));
                System.out.println("  Next Fire Time: " + triggerDetails.get("nextFireTime"));
                System.out.println("  Previous Fire Time: " + triggerDetails.get("prevFireTime"));
                System.out.println("  Priority: " + triggerDetails.get("priority"));
                System.out.println("  Misfire Instruction: " + triggerDetails.get("misfireInstr"));
                
                // Add type-specific details
                if ("CRON".equals(triggerDetails.get("type"))) {
                    System.out.println("  Cron Expression: " + triggerDetails.get("cronExpression"));
                    System.out.println("  Time Zone: " + triggerDetails.get("timeZoneId"));
                } else if ("SIMPLE".equals(triggerDetails.get("type"))) {
                    System.out.println("  Repeat Count: " + triggerDetails.get("repeatCount"));
                    System.out.println("  Repeat Interval: " + triggerDetails.get("repeatInterval") + " ms");
                    System.out.println("  Times Triggered: " + triggerDetails.get("timesTriggered"));
                }
            }
        } catch (Exception e) {
            System.out.println("Error viewing trigger: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }

    private void deleteJob(String group, String name) throws Exception {
        if (verbose) {
            System.out.println("Deleting job...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);

            QuartzDataService quartzDataService = new QuartzDataService(
                dataSource, schema, tablePrefix, schedulerName);

            // First check if we have multiple results
            List<Map<String, Object>> jobs = quartzDataService.listJobs(group, name);
            
            if (jobs.isEmpty()) {
                System.out.println("No jobs found matching the criteria");
                return;
            }

            // If we have more than one result, list them and ask for confirmation
            if (jobs.size() > 1) {
                System.out.println("\nMultiple jobs found matching the criteria:");
                listJobsWithSQL(group, name);

                if (!forceClear) {
                    System.out.println("\nWARNING: This will delete ALL matching jobs and their triggers!");
                    System.out.println("This operation cannot be undone.");
                    System.out.print("Are you sure you want to continue? (y/N): ");

                    Scanner scanner = new Scanner(System.in);
                    String response = scanner.nextLine().trim().toLowerCase();
                    if (!response.equals("y")) {
                        System.out.println("Operation cancelled.");
                        return;
                    }
                }
            } else {
                // Single job, confirm deletion
                String exactGroup = (String) jobs.get(0).get("group");
                String exactName = (String) jobs.get(0).get("name");

                if (!forceClear) {
                    System.out.println("\nWARNING: This will delete the job " + exactGroup + "." + exactName + " and its triggers!");
                    System.out.println("This operation cannot be undone.");
                    System.out.print("Are you sure you want to continue? (y/N): ");

                    Scanner scanner = new Scanner(System.in);
                    String response = scanner.nextLine().trim().toLowerCase();
                    if (!response.equals("y")) {
                        System.out.println("Operation cancelled.");
                        return;
                    }
                }

                // Set exact group and name for deletion
                group = exactGroup;
                name = exactName;
            }
            
            // Now perform the deletion
            boolean success = quartzDataService.deleteJob(group, name);
            
            if (success) {
                System.out.println("Successfully deleted job(s)");
            } else {
                System.out.println("Failed to delete job(s)");
            }
        } catch (Exception e) {
            System.out.println("Error deleting job: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }

    private void deleteTrigger(String group, String name) throws Exception {
        if (verbose) {
            System.out.println("Deleting trigger...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String triggerTableName = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "triggers" : 
                tablePrefix + "triggers";
            
            String sql = "SELECT trigger_group, trigger_name FROM " + triggerTableName + " " +
                         "WHERE 1=1 " +
                         (group != null ? "AND trigger_group LIKE ? " : "") +
                         (name != null ? "AND trigger_name LIKE ? " : "") +
                         (schedulerName != null ? "AND SCHED_NAME = ? " : "");
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                int paramIndex = 1;
                if (group != null) {
                    stmt.setString(paramIndex++, "%" + group + "%");
                }
                if (name != null) {
                    stmt.setString(paramIndex++, "%" + name + "%");
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        System.out.println("No triggers found matching the criteria");
                        return;
                    }
                    
                    // If we have more than one result, list them and ask for confirmation
                    if (rs.next()) {
                        rs.previous(); // Go back to first row
                        System.out.println("\nMultiple triggers found matching the criteria:");
                        listTriggersWithSQL(group, name);
                        
                        if (!forceClear) {
                            System.out.println("\nWARNING: This will delete ALL matching triggers!");
                            System.out.println("This operation cannot be undone.");
                            System.out.print("Are you sure you want to continue? (y/N): ");
                            
                            Scanner scanner = new Scanner(System.in);
                            String response = scanner.nextLine().trim().toLowerCase();
                            if (!response.equals("y")) {
                                System.out.println("Operation cancelled.");
                                return;
                            }
                        }
                    }
                }
            }
            
            // Now perform the deletion
            String deleteSql = "DELETE FROM " + triggerTableName + " " +
                              "WHERE 1=1 " +
                              (group != null ? "AND trigger_group LIKE ? " : "") +
                              (name != null ? "AND trigger_name LIKE ? " : "") +
                              (schedulerName != null ? "AND SCHED_NAME = ? " : "");
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(deleteSql)) {
                
                int paramIndex = 1;
                if (group != null) {
                    stmt.setString(paramIndex++, "%" + group + "%");
                }
                if (name != null) {
                    stmt.setString(paramIndex++, "%" + name + "%");
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                int rows = stmt.executeUpdate();
                System.out.println("Successfully deleted " + rows + " triggers");
            }
        } catch (Exception e) {
            System.out.println("Error deleting trigger: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) {
        int exitCode = new CommandLine(new QuartzCli()).execute(args);
        System.exit(exitCode);
    }
}