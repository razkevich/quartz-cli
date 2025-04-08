package org.razkevich.quartz;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

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

/**
 * Command-line interface for Quartz Scheduler
 */
@Command(name = "quartz-repl", 
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
    
    private void listJobsWithSQL(String groupFilter, String nameFilter) throws Exception {
        if (verbose) {
            System.out.println("Using SQL to list jobs...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String jobTableName = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "job_details" : 
                tablePrefix + "job_details";
            
            String triggerTableName = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "triggers" : 
                tablePrefix + "triggers";
            
            String sql = "SELECT j.job_group, j.job_name, j.description, j.job_class_name, " +
                        "COUNT(t.trigger_name) as trigger_count " +
                        "FROM " + jobTableName + " j " +
                        "LEFT JOIN " + triggerTableName + " t ON " +
                        "j.job_name = t.job_name AND j.job_group = t.job_group " +
                        "WHERE 1=1 " +
                        (groupFilter != null ? "AND j.job_group LIKE ? " : "") +
                        (nameFilter != null ? "AND j.job_name LIKE ? " : "") +
                        (schedulerName != null ? "AND j.SCHED_NAME = ? " : "") +
                        "GROUP BY j.job_group, j.job_name, j.description, j.job_class_name " +
                        "ORDER BY j.job_group, j.job_name";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                int paramIndex = 1;
                if (groupFilter != null) {
                    stmt.setString(paramIndex++, "%" + groupFilter + "%");
                }
                if (nameFilter != null) {
                    stmt.setString(paramIndex++, "%" + nameFilter + "%");
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (jsonOutput) {
                        System.out.println("[");
                        boolean first = true;
                        while (rs.next()) {
                            if (!first) {
                                System.out.println(",");
                            }
                            first = false;
                            System.out.println("  {");
                            System.out.println("    \"group\": \"" + rs.getString("job_group") + "\",");
                            System.out.println("    \"name\": \"" + rs.getString("job_name") + "\",");
                            System.out.println("    \"class\": \"" + rs.getString("job_class_name") + "\",");
                            System.out.println("    \"description\": \"" + rs.getString("description") + "\",");
                            System.out.println("    \"triggerCount\": " + rs.getInt("trigger_count"));
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
                        while (rs.next()) {
                            hasRows = true;
                            table.stringColumn("Group").append(rs.getString("job_group"));
                            table.stringColumn("Name").append(rs.getString("job_name"));
                            table.stringColumn("Class").append(rs.getString("job_class_name"));
                            table.stringColumn("Description").append(rs.getString("description"));
                            table.intColumn("Triggered").append(rs.getInt("trigger_count"));
                        }
                        
                        if (hasRows) {
                            System.out.println(TableFormatter.formatTable(table));
                        } else {
                            System.out.println("No jobs found.");
                        }
                    }
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
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String triggerTableName = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "triggers" : 
                tablePrefix + "triggers";
            
            String sql = "SELECT t.trigger_name, t.trigger_group, t.job_name, t.job_group, " +
                         "t.next_fire_time, t.prev_fire_time, t.trigger_type " +
                         "FROM " + triggerTableName + " t " +
                         "WHERE 1=1 " +
                         (groupFilter != null ? "AND t.trigger_group = ? " : "") +
                         (nameFilter != null ? "AND t.job_name = ? " : "") +
                         (schedulerName != null ? "AND t.SCHED_NAME = ? " : "") +
                         "ORDER BY t.next_fire_time DESC NULLS FIRST, t.trigger_group, t.trigger_name";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                int paramIndex = 1;
                if (groupFilter != null) {
                    stmt.setString(paramIndex++, groupFilter);
                }
                if (nameFilter != null) {
                    stmt.setString(paramIndex++, nameFilter);
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (jsonOutput) {
                        System.out.println("[");
                        boolean first = true;
                        while (rs.next()) {
                            if (!first) {
                                System.out.println(",");
                            }
                            first = false;
                            System.out.println("  {");
                            System.out.println("    \"group\": \"" + rs.getString("trigger_group") + "\",");
                            System.out.println("    \"name\": \"" + rs.getString("trigger_name") + "\",");
                            System.out.println("    \"job\": \"" + rs.getString("job_group") + "." + rs.getString("job_name") + "\",");
                            System.out.println("    \"type\": \"" + rs.getString("trigger_type") + "\",");
                            System.out.println("    \"nextFire\": \"" + formatRelativeTime(rs.getLong("next_fire_time")) + "\",");
                            System.out.println("    \"previousFire\": \"" + formatRelativeTime(rs.getLong("prev_fire_time")) + "\"");
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
                        while (rs.next()) {
                            hasRows = true;
                            String name = rs.getString("trigger_name");
                            String triggerGroup = rs.getString("trigger_group");
                            String jobName = rs.getString("job_name");
                            String jobGroup = rs.getString("job_group");
                            long nextFireTime = rs.getLong("next_fire_time");
                            long prevFireTime = rs.getLong("prev_fire_time");
                            String triggerType = rs.getString("trigger_type");
                            
                            String nextFire = nextFireTime > 0 ? formatRelativeTime(nextFireTime) : "";
                            String prevFire = prevFireTime > 0 ? formatRelativeTime(prevFireTime) : "";
                            
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
                }
            }
        } catch (Exception e) {
            System.out.println("Error listing triggers with SQL: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    private String formatRelativeTime(long timestamp) {
        long now = System.currentTimeMillis();
        long diff = timestamp - now;
        long absDiff = Math.abs(diff);
        
        if (absDiff < 1000) { // Less than 1 second
            return diff > 0 ? "now" : "just now";
        } else if (absDiff < 60 * 1000) { // Less than 1 minute
            long seconds = absDiff / 1000;
            return diff > 0 ? seconds + "s" : seconds + "s ago";
        } else if (absDiff < 60 * 60 * 1000) { // Less than 1 hour
            long minutes = absDiff / (60 * 1000);
            return diff > 0 ? minutes + "m" : minutes + "m ago";
        } else if (absDiff < 24 * 60 * 60 * 1000) { // Less than 1 day
            long hours = absDiff / (60 * 60 * 1000);
            return diff > 0 ? hours + "h" : hours + "h ago";
        } else {
            long days = absDiff / (24 * 60 * 60 * 1000);
            return diff > 0 ? days + "d" : days + "d ago";
        }
    }
    
    private void listRunningJobs() throws Exception {
        if (verbose) {
            System.out.println("Listing currently running jobs...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String firedTriggersTable = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "fired_triggers" : 
                tablePrefix + "fired_triggers";
            
            String sql = "SELECT ft.entry_id, ft.trigger_name, ft.trigger_group, " +
                         "ft.job_name, ft.job_group, ft.fired_time, ft.state " +
                         "FROM " + firedTriggersTable + " ft " +
                         "WHERE ft.state = 'EXECUTING' " +
                         (schedulerName != null ? "AND ft.SCHED_NAME = ? " : "") +
                         "ORDER BY ft.fired_time";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                if (schedulerName != null) {
                    stmt.setString(1, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (jsonOutput) {
                        System.out.println("[");
                        boolean first = true;
                        while (rs.next()) {
                            if (!first) {
                                System.out.println(",");
                            }
                            first = false;
                            System.out.println("  {");
                            System.out.println("    \"job\": \"" + rs.getString("job_group") + "." + rs.getString("job_name") + "\",");
                            System.out.println("    \"trigger\": \"" + rs.getString("trigger_group") + "." + rs.getString("trigger_name") + "\",");
                            System.out.println("    \"firedTime\": " + rs.getLong("fired_time") + ",");
                            System.out.println("    \"state\": \"" + rs.getString("state") + "\"");
                            System.out.print("  }");
                        }
                        System.out.println("\n]");
                    } else {
                        Table table = Table.create("Running Jobs")
                            .addColumns(
                                StringColumn.create("Job"),
                                StringColumn.create("Trigger"),
                                DateTimeColumn.create("Fired Time"),
                                StringColumn.create("State")
                            );
                        
                        while (rs.next()) {
                            table.stringColumn("Job").append(
                                rs.getString("job_group") + "." + rs.getString("job_name"));
                            table.stringColumn("Trigger").append(
                                rs.getString("trigger_group") + "." + rs.getString("trigger_name"));
                            table.dateTimeColumn("Fired Time").append(
                                Instant.ofEpochMilli(rs.getLong("fired_time"))
                                    .atZone(ZoneId.systemDefault())
                                    .toLocalDateTime());
                            table.stringColumn("State").append(rs.getString("state"));
                        }
                        
                        System.out.println(TableFormatter.formatTable(table));
                    }
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
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String pausedTriggerGroupsTable = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "paused_trigger_grps" : 
                tablePrefix + "paused_trigger_grps";
            
            String sql = "SELECT trigger_group " +
                         "FROM " + pausedTriggerGroupsTable + " " +
                         "WHERE 1=1 " +
                         (schedulerName != null ? "AND SCHED_NAME = ? " : "") +
                         "ORDER BY trigger_group";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                if (schedulerName != null) {
                    stmt.setString(1, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (jsonOutput) {
                        System.out.println("[");
                        boolean first = true;
                        while (rs.next()) {
                            if (!first) {
                                System.out.println(",");
                            }
                            first = false;
                            System.out.println("  {");
                            System.out.println("    \"group\": \"" + rs.getString("trigger_group") + "\"");
                            System.out.print("  }");
                        }
                        System.out.println("\n]");
                    } else {
                        Table table = Table.create("Paused Trigger Groups")
                            .addColumns(
                                StringColumn.create("Group")
                            );
                        
                        while (rs.next()) {
                            table.stringColumn("Group").append(rs.getString("trigger_group"));
                        }
                        
                        System.out.println(TableFormatter.formatTable(table));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error listing paused trigger groups: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
        }
    }
    
    private void listSchedulers() throws SQLException {
        DataSource dataSource = QuartzConnectionService.createDataSource(
            jdbcUrl, username, password, driver, schema);
        
        String sql = "SELECT INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL FROM " + tablePrefix + "scheduler_state ORDER BY LAST_CHECKIN_TIME DESC";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            if (jsonOutput) {
                System.out.println("[");
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        System.out.println(",");
                    }
                    first = false;
                    System.out.println("  {");
                    System.out.println("    \"instanceName\": \"" + rs.getString("instance_name") + "\",");
                    System.out.println("    \"lastCheckin\": \"" + formatRelativeTime(rs.getLong("last_checkin_time")) + "\",");
                    System.out.println("    \"checkinInterval\": " + rs.getLong("checkin_interval"));
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
                
                while (rs.next()) {
                    String instanceName = rs.getString("instance_name");
                    long lastCheckin = rs.getLong("last_checkin_time");
                    long interval = rs.getLong("checkin_interval");
                    
                    table.stringColumn("Instance Name").append(instanceName);
                    table.stringColumn("Last Checkin").append(formatRelativeTime(lastCheckin));
                    table.longColumn("Checkin Interval (ms)").append(interval);
                }
                
                System.out.println(TableFormatter.formatTable(table));
            }
        }
    }
    
    private void clearTables() throws SQLException {
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
        
        // Tables ordered by dependencies (child tables first)
        String[] tables = {
            "fired_triggers",           // No dependencies
            "paused_trigger_grps",      // No dependencies
            "locks",                    // No dependencies
            "simple_triggers",          // Depends on triggers
            "cron_triggers",            // Depends on triggers
            "blob_triggers",            // Depends on triggers
            "triggers",                 // Depends on job_details
            "job_details",              // Depends on scheduler_state
            "scheduler_state"           // No dependencies
        };
        
        DataSource dataSource = QuartzConnectionService.createDataSource(
            jdbcUrl, username, password, driver, schema);
        
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            try {
                for (String table : tables) {
                    String sql = "DELETE FROM " + tablePrefix + table;
                    if (triggerName != null && !triggerName.isEmpty()) {
                        sql += " WHERE trigger_name = ?";
                    }
                    
                    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                        if (triggerName != null && !triggerName.isEmpty()) {
                            stmt.setString(1, triggerName);
                        }
                        int rows = stmt.executeUpdate();
                        System.out.println("Cleared " + rows + " rows from " + table);
                    }
                }
                
                conn.commit();
                System.out.println("\nAll tables cleared successfully.");
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }
    
    private void viewJob(String group, String name) throws Exception {
        if (verbose) {
            System.out.println("Viewing job details...");
        }
        
        try {
            DataSource dataSource = QuartzConnectionService.createDataSource(
                jdbcUrl, username, password, driver, schema);
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String jobTableName = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "job_details" : 
                tablePrefix + "job_details";
            
            // First check if we have multiple results
            String countSql = "SELECT COUNT(*) as count FROM " + jobTableName + " " +
                             "WHERE 1=1 " +
                             (group != null ? "AND job_group LIKE ? " : "") +
                             (name != null ? "AND job_name LIKE ? " : "") +
                             (schedulerName != null ? "AND SCHED_NAME = ? " : "");
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement countStmt = conn.prepareStatement(countSql)) {
                
                int paramIndex = 1;
                if (group != null) {
                    countStmt.setString(paramIndex++, "%" + group + "%");
                }
                if (name != null) {
                    countStmt.setString(paramIndex++, "%" + name + "%");
                }
                if (schedulerName != null) {
                    countStmt.setString(paramIndex++, schedulerName);
                }
                
                try (ResultSet countRs = countStmt.executeQuery()) {
                    countRs.next();
                    int count = countRs.getInt("count");
                    
                    if (count == 0) {
                        System.out.println("No jobs found matching the criteria");
                        return;
                    }
                    
                    if (count > 1) {
                        // Multiple results found, switch to list mode
                        listJobsWithSQL(group, name);
                        return;
                    }
                }
            }
            
            // Now get the single result
            String sql = "SELECT * FROM " + jobTableName + " " +
                         "WHERE 1=1 " +
                         (group != null ? "AND job_group LIKE ? " : "") +
                         (name != null ? "AND job_name LIKE ? " : "") +
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
                        System.out.println("No jobs found matching the criteria");
                        return;
                    }
                    
                    String jobData = rs.getString("job_data");
                    String decodedJobData = jobData;
                    try {
                        // First try Base64 decoding
                        try {
                            byte[] decodedBytes = java.util.Base64.getDecoder().decode(jobData);
                            decodedJobData = new String(decodedBytes);
                        } catch (IllegalArgumentException e) {
                            // If not Base64, try hex decoding
                            if (jobData.startsWith("\\x")) {
                                jobData = jobData.substring(2); // Remove \x prefix
                                byte[] bytes = new byte[jobData.length() / 2];
                                for (int i = 0; i < bytes.length; i++) {
                                    bytes[i] = (byte) Integer.parseInt(jobData.substring(2 * i, 2 * i + 2), 16);
                                }
                                decodedJobData = new String(bytes);
                            }
                        }
                    } catch (Exception e) {
                        // If decoding fails, use original data
                        if (verbose) {
                            System.out.println("Warning: Could not decode job data: " + e.getMessage());
                        }
                    }
                    
                    if (jsonOutput) {
                        System.out.println("{");
                        System.out.println("  \"group\": \"" + rs.getString("job_group") + "\",");
                        System.out.println("  \"name\": \"" + rs.getString("job_name") + "\",");
                        System.out.println("  \"description\": \"" + rs.getString("description") + "\",");
                        System.out.println("  \"jobClass\": \"" + rs.getString("job_class_name") + "\",");
                        System.out.println("  \"isDurable\": " + rs.getBoolean("is_durable") + ",");
                        System.out.println("  \"isNonConcurrent\": " + rs.getBoolean("is_nonconcurrent") + ",");
                        System.out.println("  \"isUpdateData\": " + rs.getBoolean("is_update_data") + ",");
                        System.out.println("  \"requestsRecovery\": " + rs.getBoolean("requests_recovery") + ",");
                        System.out.println("  \"jobData\": \"" + decodedJobData + "\"");
                        System.out.println("}");
                    } else {
                        Table table = Table.create("Job Details")
                            .addColumns(
                                StringColumn.create("Property"),
                                StringColumn.create("Value")
                            );
                        
                        table.stringColumn("Property").append("Group");
                        table.stringColumn("Value").append(rs.getString("job_group"));
                        
                        table.stringColumn("Property").append("Name");
                        table.stringColumn("Value").append(rs.getString("job_name"));
                        
                        table.stringColumn("Property").append("Description");
                        table.stringColumn("Value").append(rs.getString("description"));
                        
                        table.stringColumn("Property").append("Job Class");
                        table.stringColumn("Value").append(rs.getString("job_class_name"));
                        
                        table.stringColumn("Property").append("Is Durable");
                        table.stringColumn("Value").append(String.valueOf(rs.getBoolean("is_durable")));
                        
                        table.stringColumn("Property").append("Is Non-Concurrent");
                        table.stringColumn("Value").append(String.valueOf(rs.getBoolean("is_nonconcurrent")));
                        
                        table.stringColumn("Property").append("Is Update Data");
                        table.stringColumn("Value").append(String.valueOf(rs.getBoolean("is_update_data")));
                        
                        table.stringColumn("Property").append("Requests Recovery");
                        table.stringColumn("Value").append(String.valueOf(rs.getBoolean("requests_recovery")));
                        
                        table.stringColumn("Property").append("Job Data");
                        table.stringColumn("Value").append(decodedJobData);
                        
                        System.out.println(TableFormatter.formatTable(table));
                    }
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
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String triggerTableName = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "triggers" : 
                tablePrefix + "triggers";
            
            String sql = "SELECT t.*, " +
                         "CASE t.trigger_type " +
                         "  WHEN 'CRON' THEN (SELECT CAST(cron_expression AS text) FROM " + tablePrefix + "cron_triggers WHERE trigger_name = t.trigger_name AND trigger_group = t.trigger_group) " +
                         "  WHEN 'SIMPLE' THEN (SELECT CAST(repeat_interval AS text) FROM " + tablePrefix + "simple_triggers WHERE trigger_name = t.trigger_name AND trigger_group = t.trigger_group) " +
                         "END as schedule_info " +
                         "FROM " + triggerTableName + " t " +
                         "WHERE 1=1 " +
                         (group != null ? "AND t.trigger_group LIKE ? " : "") +
                         (name != null ? "AND t.trigger_name LIKE ? " : "") +
                         (schedulerName != null ? "AND t.SCHED_NAME = ? " : "");
            
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
                    
                    // If we have more than one result, switch to list mode
                    if (rs.next()) {
                        rs.previous(); // Go back to first row
                        listTriggersWithSQL(group, name);
                        return;
                    }
                    
                    // Single result, show detailed view
                    rs.first();
                    if (jsonOutput) {
                        System.out.println("{");
                        System.out.println("  \"group\": \"" + rs.getString("trigger_group") + "\",");
                        System.out.println("  \"name\": \"" + rs.getString("trigger_name") + "\",");
                        System.out.println("  \"jobGroup\": \"" + rs.getString("job_group") + "\",");
                        System.out.println("  \"jobName\": \"" + rs.getString("job_name") + "\",");
                        System.out.println("  \"description\": \"" + rs.getString("description") + "\",");
                        System.out.println("  \"nextFireTime\": " + rs.getLong("next_fire_time") + ",");
                        System.out.println("  \"prevFireTime\": " + rs.getLong("prev_fire_time") + ",");
                        System.out.println("  \"priority\": " + rs.getInt("priority") + ",");
                        System.out.println("  \"triggerState\": \"" + rs.getString("trigger_state") + "\",");
                        System.out.println("  \"triggerType\": \"" + rs.getString("trigger_type") + "\",");
                        System.out.println("  \"startTime\": " + rs.getLong("start_time") + ",");
                        System.out.println("  \"endTime\": " + rs.getLong("end_time") + ",");
                        System.out.println("  \"calendarName\": \"" + rs.getString("calendar_name") + "\",");
                        System.out.println("  \"misfireInstruction\": " + rs.getInt("misfire_instr") + ",");
                        System.out.println("  \"jobData\": \"" + rs.getString("job_data") + "\",");
                        System.out.println("  \"scheduleInfo\": \"" + rs.getString("schedule_info") + "\"");
                        System.out.println("}");
                    } else {
                        Table table = Table.create("Trigger Details")
                            .addColumns(
                                StringColumn.create("Property"),
                                StringColumn.create("Value")
                            );
                        
                        table.stringColumn("Property").append("Group");
                        table.stringColumn("Value").append(rs.getString("trigger_group"));
                        
                        table.stringColumn("Property").append("Name");
                        table.stringColumn("Value").append(rs.getString("trigger_name"));
                        
                        table.stringColumn("Property").append("Job Group");
                        table.stringColumn("Value").append(rs.getString("job_group"));
                        
                        table.stringColumn("Property").append("Job Name");
                        table.stringColumn("Value").append(rs.getString("job_name"));
                        
                        table.stringColumn("Property").append("Description");
                        table.stringColumn("Value").append(rs.getString("description"));
                        
                        table.stringColumn("Property").append("Next Fire Time");
                        table.stringColumn("Value").append(formatRelativeTime(rs.getLong("next_fire_time")));
                        
                        table.stringColumn("Property").append("Previous Fire Time");
                        table.stringColumn("Value").append(formatRelativeTime(rs.getLong("prev_fire_time")));
                        
                        table.stringColumn("Property").append("Priority");
                        table.stringColumn("Value").append(String.valueOf(rs.getInt("priority")));
                        
                        table.stringColumn("Property").append("State");
                        table.stringColumn("Value").append(rs.getString("trigger_state"));
                        
                        table.stringColumn("Property").append("Type");
                        table.stringColumn("Value").append(rs.getString("trigger_type"));
                        
                        table.stringColumn("Property").append("Start Time");
                        table.stringColumn("Value").append(formatRelativeTime(rs.getLong("start_time")));
                        
                        table.stringColumn("Property").append("End Time");
                        table.stringColumn("Value").append(formatRelativeTime(rs.getLong("end_time")));
                        
                        table.stringColumn("Property").append("Calendar Name");
                        table.stringColumn("Value").append(rs.getString("calendar_name"));
                        
                        table.stringColumn("Property").append("Misfire Instruction");
                        table.stringColumn("Value").append(String.valueOf(rs.getInt("misfire_instr")));
                        
                        table.stringColumn("Property").append("Job Data");
                        String jobData = rs.getString("job_data");
                        try {
                            byte[] decodedBytes = java.util.Base64.getDecoder().decode(jobData);
                            jobData = new String(decodedBytes);
                        } catch (IllegalArgumentException e) {
                            // Not a Base64-encoded string, use original
                        }
                        table.stringColumn("Value").append(jobData);
                        
                        table.stringColumn("Property").append("Schedule Info");
                        table.stringColumn("Value").append(rs.getString("schedule_info"));
                        
                        System.out.println(TableFormatter.formatTable(table));
                    }
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
            
            String tablePrefix = this.tablePrefix.toLowerCase();
            String jobTableName = (schema != null && !schema.isEmpty()) ? 
                schema + "." + tablePrefix + "job_details" : 
                tablePrefix + "job_details";
            
            String sql = "SELECT job_group, job_name FROM " + jobTableName + " " +
                         "WHERE 1=1 " +
                         (group != null ? "AND job_group LIKE ? " : "") +
                         (name != null ? "AND job_name LIKE ? " : "") +
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
                        System.out.println("No jobs found matching the criteria");
                        return;
                    }
                    
                    // If we have more than one result, list them and ask for confirmation
                    if (rs.next()) {
                        rs.previous(); // Go back to first row
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
                    }
                }
            }
            
            // Now perform the deletion
            String deleteSql = "DELETE FROM " + jobTableName + " " +
                              "WHERE 1=1 " +
                              (group != null ? "AND job_group LIKE ? " : "") +
                              (name != null ? "AND job_name LIKE ? " : "") +
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
                System.out.println("Successfully deleted " + rows + " jobs");
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