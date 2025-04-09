package org.razkevich.quartz;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for accessing Quartz database data
 * This class centralizes all database operations to avoid code duplication
 */
public class QuartzDataService {
    
    private final DataSource dataSource;
    private final String schema;
    private final String tablePrefix;
    private final String schedulerName;
    
    /**
     * Create a new QuartzDataService with the specified parameters
     */
    public QuartzDataService(DataSource dataSource, String schema, String tablePrefix, String schedulerName) {
        this.dataSource = dataSource;
        this.schema = schema;
        this.tablePrefix = tablePrefix.toLowerCase();
        this.schedulerName = schedulerName;
    }
    
    /**
     * Get the fully qualified table name with schema and prefix
     */
    public String getTableName(String tableName) {
        return (schema != null && !schema.isEmpty()) ? 
            schema + "." + tablePrefix + tableName : 
            tablePrefix + tableName;
    }
    
    /**
     * Format a timestamp in milliseconds to a readable date/time string
     */
    public String formatTimestamp(long timestamp) {
        if (timestamp <= 0) {
            return "N/A";
        }
        return LocalDateTime.ofInstant(
            Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()).toString();
    }
    
    /**
     * Get a list of all jobs
     */
    public List<Map<String, Object>> listJobs(String groupFilter, String nameFilter) {
        List<Map<String, Object>> jobs = new ArrayList<>();
        
        try {
            String jobTableName = getTableName("job_details");
            String triggerTableName = getTableName("triggers");
            
            String sql = "SELECT j.SCHED_NAME, j.job_group, j.job_name, j.description, j.job_class_name, " +
                        "COUNT(t.trigger_name) as trigger_count " +
                        "FROM " + jobTableName + " j " +
                        "LEFT JOIN " + triggerTableName + " t ON " +
                        "j.job_name = t.job_name AND j.job_group = t.job_group AND j.SCHED_NAME = t.SCHED_NAME " +
                        "WHERE 1=1 " +
                        (groupFilter != null ? "AND j.job_group LIKE ? " : "") +
                        (nameFilter != null ? "AND j.job_name LIKE ? " : "") +
                        (schedulerName != null ? "AND j.SCHED_NAME = ? " : "") +
                        "GROUP BY j.SCHED_NAME, j.job_group, j.job_name, j.description, j.job_class_name " +
                        "ORDER BY j.SCHED_NAME, j.job_group, j.job_name";
            
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
                    while (rs.next()) {
                        Map<String, Object> job = new HashMap<>();
                        job.put("group", rs.getString("job_group"));
                        job.put("name", rs.getString("job_name"));
                        job.put("class", rs.getString("job_class_name"));
                        job.put("description", rs.getString("description"));
                        job.put("triggerCount", rs.getInt("trigger_count"));
                        job.put("schedulerName", rs.getString("SCHED_NAME"));
                        jobs.add(job);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error listing jobs: " + e.getMessage(), e);
        }
        
        return jobs;
    }
    
    /**
     * Get a list of all triggers
     */
    public List<Map<String, Object>> listTriggers(String groupFilter, String nameFilter) {
        List<Map<String, Object>> triggers = new ArrayList<>();
        
        try {
            String triggerTableName = getTableName("triggers");
            
            String sql = "SELECT t.SCHED_NAME, t.trigger_name, t.trigger_group, t.job_name, t.job_group, " +
                         "t.next_fire_time, t.prev_fire_time, t.trigger_type, t.trigger_state, " +
                         "t.start_time, t.end_time, t.priority, t.misfire_instr, t.job_data " +
                         "FROM " + triggerTableName + " t " +
                         "WHERE 1=1 " +
                         (groupFilter != null ? "AND t.trigger_group LIKE ? " : "") +
                         (nameFilter != null ? "AND t.trigger_name LIKE ? " : "") +
                         (schedulerName != null ? "AND t.SCHED_NAME = ? " : "") +
                         "ORDER BY t.next_fire_time DESC NULLS FIRST, t.trigger_group, t.trigger_name";
            
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
                    while (rs.next()) {
                        Map<String, Object> trigger = new HashMap<>();
                        trigger.put("name", rs.getString("trigger_name"));
                        trigger.put("group", rs.getString("trigger_group"));
                        trigger.put("jobName", rs.getString("job_name"));
                        trigger.put("jobGroup", rs.getString("job_group"));
                        trigger.put("state", rs.getString("trigger_state"));
                        trigger.put("type", rs.getString("trigger_type"));
                        trigger.put("schedulerName", rs.getString("SCHED_NAME"));
                        
                        // Format timestamps
                        long nextFireTime = rs.getLong("next_fire_time");
                        if (!rs.wasNull() && nextFireTime > 0) {
                            trigger.put("nextFireTime", formatTimestamp(nextFireTime));
                            trigger.put("nextFireTimeMs", nextFireTime);
                        } else {
                            trigger.put("nextFireTime", "N/A");
                            trigger.put("nextFireTimeMs", 0);
                        }
                        
                        long prevFireTime = rs.getLong("prev_fire_time");
                        if (!rs.wasNull() && prevFireTime > 0) {
                            trigger.put("prevFireTime", formatTimestamp(prevFireTime));
                            trigger.put("prevFireTimeMs", prevFireTime);
                        } else {
                            trigger.put("prevFireTime", "N/A");
                            trigger.put("prevFireTimeMs", 0);
                        }
                        
                        triggers.add(trigger);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error listing triggers: " + e.getMessage(), e);
        }
        
        return triggers;
    }
    
    /**
     * Get a list of all running jobs
     */
    public List<Map<String, Object>> listRunningJobs() {
        List<Map<String, Object>> runningJobs = new ArrayList<>();
        
        try {
            String firedTriggersTableName = getTableName("fired_triggers");
            
            String sql = "SELECT * FROM " + firedTriggersTableName + " " +
                         "WHERE 1=1 " +
                         (schedulerName != null ? "AND SCHED_NAME = ? " : "") +
                         "ORDER BY FIRED_TIME DESC";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                if (schedulerName != null) {
                    stmt.setString(1, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> job = new HashMap<>();
                        job.put("schedulerName", rs.getString("SCHED_NAME"));
                        job.put("entryId", rs.getString("ENTRY_ID"));
                        job.put("triggerName", rs.getString("TRIGGER_NAME"));
                        job.put("triggerGroup", rs.getString("TRIGGER_GROUP"));
                        job.put("instanceName", rs.getString("INSTANCE_NAME"));
                        job.put("firedTime", formatTimestamp(rs.getLong("FIRED_TIME")));
                        job.put("firedTimeMs", rs.getLong("FIRED_TIME"));
                        job.put("scheduledTime", formatTimestamp(rs.getLong("SCHED_TIME")));
                        job.put("scheduledTimeMs", rs.getLong("SCHED_TIME"));
                        job.put("priority", rs.getInt("PRIORITY"));
                        job.put("state", rs.getString("STATE"));
                        job.put("jobName", rs.getString("JOB_NAME"));
                        job.put("jobGroup", rs.getString("JOB_GROUP"));
                        job.put("isNonConcurrent", rs.getString("IS_NONCONCURRENT"));
                        job.put("requestsRecovery", rs.getString("REQUESTS_RECOVERY"));
                        runningJobs.add(job);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error listing running jobs: " + e.getMessage(), e);
        }
        
        return runningJobs;
    }
    
    /**
     * Get a list of all paused trigger groups
     */
    public List<Map<String, Object>> listPausedTriggerGroups() {
        List<Map<String, Object>> pausedGroups = new ArrayList<>();
        
        try {
            String pausedTriggerGrpsTableName = getTableName("paused_trigger_grps");
            
            String sql = "SELECT * FROM " + pausedTriggerGrpsTableName + " " +
                         "WHERE 1=1 " +
                         (schedulerName != null ? "AND SCHED_NAME = ? " : "") +
                         "ORDER BY TRIGGER_GROUP";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                if (schedulerName != null) {
                    stmt.setString(1, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> group = new HashMap<>();
                        group.put("schedulerName", rs.getString("SCHED_NAME"));
                        group.put("triggerGroup", rs.getString("TRIGGER_GROUP"));
                        pausedGroups.add(group);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error listing paused trigger groups: " + e.getMessage(), e);
        }
        
        return pausedGroups;
    }
    
    /**
     * Get a list of all schedulers
     */
    public List<Map<String, Object>> listSchedulers() {
        List<Map<String, Object>> schedulers = new ArrayList<>();
        
        try {
            String schedulersTableName = getTableName("scheduler_state");
            
            String sql = "SELECT * FROM " + schedulersTableName + " " +
                         "ORDER BY SCHED_NAME, INSTANCE_NAME";
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {
                
                while (rs.next()) {
                    Map<String, Object> scheduler = new HashMap<>();
                    scheduler.put("schedulerName", rs.getString("SCHED_NAME"));
                    scheduler.put("instanceName", rs.getString("INSTANCE_NAME"));
                    scheduler.put("lastCheckinTime", formatTimestamp(rs.getLong("LAST_CHECKIN_TIME")));
                    scheduler.put("lastCheckinTimeMs", rs.getLong("LAST_CHECKIN_TIME"));
                    scheduler.put("checkinInterval", rs.getLong("CHECKIN_INTERVAL"));
                    schedulers.add(scheduler);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error listing schedulers: " + e.getMessage(), e);
        }
        
        return schedulers;
    }
    
    /**
     * Get detailed information about a job
     */
    public Map<String, Object> getJobDetails(String group, String name) {
        Map<String, Object> jobDetails = new HashMap<>();
        
        try {
            String jobTableName = getTableName("job_details");
            String triggerTableName = getTableName("triggers");
            
            // Get job details
            String jobSql = "SELECT * FROM " + jobTableName + " " +
                           "WHERE 1=1 " +
                           (group != null ? "AND job_group = ? " : "") +
                           (name != null ? "AND job_name = ? " : "") +
                           (schedulerName != null ? "AND SCHED_NAME = ? " : "");
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(jobSql)) {
                
                int paramIndex = 1;
                if (group != null) {
                    stmt.setString(paramIndex++, group);
                }
                if (name != null) {
                    stmt.setString(paramIndex++, name);
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        jobDetails.put("name", rs.getString("job_name"));
                        jobDetails.put("group", rs.getString("job_group"));
                        jobDetails.put("description", rs.getString("description"));
                        jobDetails.put("jobClass", rs.getString("job_class_name"));
                        jobDetails.put("schedulerName", rs.getString("SCHED_NAME"));
                        jobDetails.put("isDurable", rs.getString("is_durable"));
                        jobDetails.put("isNonConcurrent", rs.getString("is_nonconcurrent"));
                        jobDetails.put("isUpdateData", rs.getString("is_update_data"));
                        jobDetails.put("requestsRecovery", rs.getString("requests_recovery"));
                        
                        // Get job data map
                        byte[] jobData = rs.getBytes("job_data");
                        if (jobData != null) {
                            jobDetails.put("hasJobData", true);
                        } else {
                            jobDetails.put("hasJobData", false);
                        }
                    }
                }
            }
            
            // Get associated triggers
            if (jobDetails.containsKey("name") && jobDetails.containsKey("group")) {
                // Get the scheduler name from the job details
                String jobSchedulerName = (String) jobDetails.get("schedulerName");
                
                String triggerSql = "SELECT * FROM " + triggerTableName + " " +
                                   "WHERE job_name = ? AND job_group = ? " +
                                   "AND SCHED_NAME = ? " +
                                   "ORDER BY next_fire_time";
                
                List<Map<String, Object>> triggers = new ArrayList<>();
                
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement stmt = conn.prepareStatement(triggerSql)) {
                    
                    stmt.setString(1, (String) jobDetails.get("name"));
                    stmt.setString(2, (String) jobDetails.get("group"));
                    stmt.setString(3, jobSchedulerName);
                    
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            Map<String, Object> trigger = new HashMap<>();
                            trigger.put("name", rs.getString("trigger_name"));
                            trigger.put("group", rs.getString("trigger_group"));
                            trigger.put("state", rs.getString("trigger_state"));
                            trigger.put("type", rs.getString("trigger_type"));
                            
                            // Format timestamps
                            long nextFireTime = rs.getLong("next_fire_time");
                            if (!rs.wasNull() && nextFireTime > 0) {
                                trigger.put("nextFireTime", formatTimestamp(nextFireTime));
                            } else {
                                trigger.put("nextFireTime", "N/A");
                            }
                            
                            long prevFireTime = rs.getLong("prev_fire_time");
                            if (!rs.wasNull() && prevFireTime > 0) {
                                trigger.put("prevFireTime", formatTimestamp(prevFireTime));
                            } else {
                                trigger.put("prevFireTime", "N/A");
                            }
                            
                            long startTime = rs.getLong("start_time");
                            if (!rs.wasNull() && startTime > 0) {
                                trigger.put("startTime", formatTimestamp(startTime));
                            } else {
                                trigger.put("startTime", "N/A");
                            }
                            
                            long endTime = rs.getLong("end_time");
                            if (!rs.wasNull() && endTime > 0) {
                                trigger.put("endTime", formatTimestamp(endTime));
                            } else {
                                trigger.put("endTime", "N/A");
                            }
                            
                            triggers.add(trigger);
                        }
                    }
                }
                
                jobDetails.put("triggers", triggers);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Error getting job details: " + e.getMessage(), e);
        }
        
        return jobDetails;
    }
    
    /**
     * Get detailed information about a trigger
     */
    public Map<String, Object> getTriggerDetails(String group, String name) {
        Map<String, Object> triggerDetails = new HashMap<>();
        
        try {
            String triggerTableName = getTableName("triggers");
            String cronTableName = getTableName("cron_triggers");
            String simpleTriggerTableName = getTableName("simple_triggers");
            
            // Get trigger details
            String triggerSql = "SELECT * FROM " + triggerTableName + " " +
                               "WHERE 1=1 " +
                               (group != null ? "AND trigger_group = ? " : "") +
                               (name != null ? "AND trigger_name = ? " : "") +
                               (schedulerName != null ? "AND SCHED_NAME = ? " : "");
            
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(triggerSql)) {
                
                int paramIndex = 1;
                if (group != null) {
                    stmt.setString(paramIndex++, group);
                }
                if (name != null) {
                    stmt.setString(paramIndex++, name);
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        triggerDetails.put("name", rs.getString("trigger_name"));
                        triggerDetails.put("group", rs.getString("trigger_group"));
                        triggerDetails.put("jobName", rs.getString("job_name"));
                        triggerDetails.put("jobGroup", rs.getString("job_group"));
                        triggerDetails.put("description", rs.getString("description"));
                        triggerDetails.put("schedulerName", rs.getString("SCHED_NAME"));
                        triggerDetails.put("state", rs.getString("trigger_state"));
                        triggerDetails.put("type", rs.getString("trigger_type"));
                        triggerDetails.put("priority", rs.getInt("priority"));
                        
                        // Format timestamps
                        long nextFireTime = rs.getLong("next_fire_time");
                        if (!rs.wasNull() && nextFireTime > 0) {
                            triggerDetails.put("nextFireTime", formatTimestamp(nextFireTime));
                            triggerDetails.put("nextFireTimeMs", nextFireTime);
                        } else {
                            triggerDetails.put("nextFireTime", "N/A");
                            triggerDetails.put("nextFireTimeMs", 0);
                        }
                        
                        long prevFireTime = rs.getLong("prev_fire_time");
                        if (!rs.wasNull() && prevFireTime > 0) {
                            triggerDetails.put("prevFireTime", formatTimestamp(prevFireTime));
                            triggerDetails.put("prevFireTimeMs", prevFireTime);
                        } else {
                            triggerDetails.put("prevFireTime", "N/A");
                            triggerDetails.put("prevFireTimeMs", 0);
                        }
                        
                        long startTime = rs.getLong("start_time");
                        if (!rs.wasNull() && startTime > 0) {
                            triggerDetails.put("startTime", formatTimestamp(startTime));
                            triggerDetails.put("startTimeMs", startTime);
                        } else {
                            triggerDetails.put("startTime", "N/A");
                            triggerDetails.put("startTimeMs", 0);
                        }
                        
                        long endTime = rs.getLong("end_time");
                        if (!rs.wasNull() && endTime > 0) {
                            triggerDetails.put("endTime", formatTimestamp(endTime));
                            triggerDetails.put("endTimeMs", endTime);
                        } else {
                            triggerDetails.put("endTime", "N/A");
                            triggerDetails.put("endTimeMs", 0);
                        }
                        
                        // Get trigger data map
                        byte[] triggerData = rs.getBytes("job_data");
                        if (triggerData != null) {
                            triggerDetails.put("hasJobData", true);
                        } else {
                            triggerDetails.put("hasJobData", false);
                        }
                        
                        // Get specific trigger type details
                        String triggerType = rs.getString("trigger_type");
                        if ("CRON".equals(triggerType)) {
                            String cronSql = "SELECT * FROM " + cronTableName + " " +
                                           "WHERE trigger_name = ? AND trigger_group = ? " +
                                           (schedulerName != null ? "AND SCHED_NAME = ? " : "");
                            
                            try (PreparedStatement cronStmt = conn.prepareStatement(cronSql)) {
                                cronStmt.setString(1, rs.getString("trigger_name"));
                                cronStmt.setString(2, rs.getString("trigger_group"));
                                if (schedulerName != null) {
                                    cronStmt.setString(3, schedulerName);
                                }
                                
                                try (ResultSet cronRs = cronStmt.executeQuery()) {
                                    if (cronRs.next()) {
                                        triggerDetails.put("cronExpression", cronRs.getString("cron_expression"));
                                        triggerDetails.put("timeZoneId", cronRs.getString("time_zone_id"));
                                    }
                                }
                            }
                        } else if ("SIMPLE".equals(triggerType)) {
                            String simpleSql = "SELECT * FROM " + simpleTriggerTableName + " " +
                                             "WHERE trigger_name = ? AND trigger_group = ? " +
                                             (schedulerName != null ? "AND SCHED_NAME = ? " : "");
                            
                            try (PreparedStatement simpleStmt = conn.prepareStatement(simpleSql)) {
                                simpleStmt.setString(1, rs.getString("trigger_name"));
                                simpleStmt.setString(2, rs.getString("trigger_group"));
                                if (schedulerName != null) {
                                    simpleStmt.setString(3, schedulerName);
                                }
                                
                                try (ResultSet simpleRs = simpleStmt.executeQuery()) {
                                    if (simpleRs.next()) {
                                        triggerDetails.put("repeatCount", simpleRs.getInt("repeat_count"));
                                        triggerDetails.put("repeatInterval", simpleRs.getLong("repeat_interval"));
                                        triggerDetails.put("timesTriggered", simpleRs.getInt("times_triggered"));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Error getting trigger details: " + e.getMessage(), e);
        }
        
        return triggerDetails;
    }
    
    /**
     * Delete a job and its triggers
     */
    public boolean deleteJob(String group, String name) {
        try {
            String jobTableName = getTableName("job_details");

            String sql = "DELETE FROM " + jobTableName + " " +
                         "WHERE 1=1 " +
                         (group != null ? "AND job_group = ? " : "") +
                         (name != null ? "AND job_name = ? " : "") +
                         (schedulerName != null ? "AND SCHED_NAME = ? " : "");

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                int paramIndex = 1;
                if (group != null) {
                    stmt.setString(paramIndex++, group);
                }
                if (name != null) {
                    stmt.setString(paramIndex++, name);
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                int rowsAffected = stmt.executeUpdate();
                return rowsAffected > 0;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error deleting job: " + e.getMessage(), e);
        }
    }
    
    /**
     * Delete a trigger
     */
    public boolean deleteTrigger(String group, String name) {
        try {
            String triggerTableName = getTableName("triggers");

            String sql = "DELETE FROM " + triggerTableName + " " +
                         "WHERE 1=1 " +
                         (group != null ? "AND trigger_group = ? " : "") +
                         (name != null ? "AND trigger_name = ? " : "") +
                         (schedulerName != null ? "AND SCHED_NAME = ? " : "");

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                int paramIndex = 1;
                if (group != null) {
                    stmt.setString(paramIndex++, group);
                }
                if (name != null) {
                    stmt.setString(paramIndex++, name);
                }
                if (schedulerName != null) {
                    stmt.setString(paramIndex++, schedulerName);
                }
                
                int rowsAffected = stmt.executeUpdate();
                return rowsAffected > 0;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error deleting trigger: " + e.getMessage(), e);
        }
    }
    
    /**
     * Clear all Quartz tables
     */
    public void clearTables() {
        try {
            String[] tables = {
                "fired_triggers",
                "paused_trigger_grps",
                "scheduler_state",
                "locks",
                "simple_triggers",
                "simprop_triggers",
                "cron_triggers",
                "blob_triggers",
                "triggers",
                "job_details",
                "calendars"
            };
            
            try (Connection conn = dataSource.getConnection()) {
                for (String table : tables) {
                    String tableName = getTableName(table);
                    String sql = "DELETE FROM " + tableName;
                    if (schedulerName != null) {
                        sql += " WHERE SCHED_NAME = ?";
                    }
                    
                    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                        if (schedulerName != null) {
                            stmt.setString(1, schedulerName);
                        }
                        stmt.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error clearing tables: " + e.getMessage(), e);
        }
    }
}