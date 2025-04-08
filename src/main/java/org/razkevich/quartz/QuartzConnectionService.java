package org.razkevich.quartz;

import org.apache.commons.dbcp2.BasicDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Service to manage connections to Quartz scheduler databases
 */
public class QuartzConnectionService {
    
    /**
     * Create a DataSource with the specified parameters
     */
    public static DataSource createDataSource(String jdbcUrl, String username, String password, 
                                           String driver, String schema) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driver);
        
        // Set connection pool properties
        dataSource.setInitialSize(1);
        dataSource.setMaxTotal(5);
        dataSource.setMaxIdle(2);
        dataSource.setMinIdle(1);
        dataSource.setMaxWaitMillis(5000);
        
        // Set PostgreSQL-specific properties
        if (jdbcUrl.contains("postgresql")) {
            if (schema != null && !schema.isEmpty()) {
                dataSource.setConnectionInitSqls(Arrays.asList("SET search_path TO " + schema));
            }
        }
        
        return dataSource;
    }
    
    /**
     * Test the connection to the database
     */
    public static boolean testConnection(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
} 