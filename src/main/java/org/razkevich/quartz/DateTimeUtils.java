package org.razkevich.quartz;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
public class DateTimeUtils {

    @Deprecated
    public static String formatDate(Date date) {
        if (date == null) {
            return "";
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.format(date);
    }

    /**
     * Formats a timestamp into a human-readable relative time string.
     * For example: "just now", "5 minutes ago", "2 hours ago", "yesterday", "5 days ago".
     *
     * @param timestamp The timestamp in milliseconds since epoch
     * @return A human-readable relative time string
     */
    public static String formatRelativeTime(long timestamp) {
        // Convert timestamp to LocalDateTime
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
        );

        LocalDateTime now = LocalDateTime.now();

        // Calculate the difference
        long secondsAgo = ChronoUnit.SECONDS.between(time, now);
        long minutesAgo = ChronoUnit.MINUTES.between(time, now);
        long hoursAgo = ChronoUnit.HOURS.between(time, now);
        long daysAgo = ChronoUnit.DAYS.between(time, now);

        // Format the relative time
        if (secondsAgo < 60) {
            return "just now";
        } else if (minutesAgo < 60) {
            return minutesAgo == 1 ? "1 minute ago" : minutesAgo + " minutes ago";
        } else if (hoursAgo < 24) {
            return hoursAgo == 1 ? "1 hour ago" : hoursAgo + " hours ago";
        } else if (daysAgo < 7) {
            if (daysAgo == 1) {
                return "yesterday";
            } else {
                return daysAgo + " days ago";
            }
        } else if (daysAgo < 30) {
            long weeksAgo = daysAgo / 7;
            return weeksAgo == 1 ? "1 week ago" : weeksAgo + " weeks ago";
        } else if (daysAgo < 365) {
            long monthsAgo = daysAgo / 30;
            return monthsAgo == 1 ? "1 month ago" : monthsAgo + " months ago";
        } else {
            long yearsAgo = daysAgo / 365;
            return yearsAgo == 1 ? "1 year ago" : yearsAgo + " years ago";
        }
    }

    public static String formatTimestamp(long timestamp) {
        Date date = new Date(timestamp);
        return formatDate(date);
    }
}