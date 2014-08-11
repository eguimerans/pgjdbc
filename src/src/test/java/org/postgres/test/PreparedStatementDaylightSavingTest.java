package org.postgres.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import junit.framework.TestCase;

import org.postgresql.test.TestUtil;

public class PreparedStatementDaylightSavingTest extends TestCase {

    private Connection _conn;

    private String dateTimeInDaylightSavingDay;

    /** Date object for dateTimeInDaylightSavingDay */
    private Date date;

    /**
     * Hour that will produce this problem.
     *
     * Problematic hours for GMT-5 when DST starts are 3AM, 4AM, 5AM and 6AM (In 2014 DST starts on March, 9th at 2AM).
     * Problematic hours for GMT-5 when DST ends are 2AM, 3AM, 4AM and 5AM (In 2014 DST ends on November, 2nd at 2AM).
     *
     */
    private int testHour = 4;

    private String uniqueTestName;

    @Override
    protected void setUp() throws Exception {

        //_conn = TestUtil.openDB();

        String url = "jdbc:postgresql://localhost/test";
        String user = "postgres";
        String password = "root";
        _conn = DriverManager.getConnection(url, user, password);
        TestUtil.createTable(_conn, "dls1test", "name varchar(500), val timestamp");

        // daylight saving time in EDT  2014-03-09  from 2:00 jumps to -> 3:00
        //http://www.timetemperature.com/canada/daylight_saving_time_canada.shtml

        String hh = String.valueOf(testHour);
        if (hh.length() < 2) {
            hh = "0" + hh;
        }
        dateTimeInDaylightSavingDay = "2014-03-09 " + testHour + ":21:28";

        // Other dates with the same problem
        //dateTimeInSaylightSavingDay = "2009-03-08 06:21:28";
        //dateTimeInSaylightSavingDay = "2010-03-14 06:21:28";
        //dateTimeInSaylightSavingDay = "2015-03-08 06:21:28";

        //Setup test data
        uniqueTestName = "Test#" + System.nanoTime();

        // Save event in day when daylight saving change happened
        {
            PreparedStatement stmt = _conn.prepareStatement("INSERT INTO dls1test (name, val)  VALUES (?, ?)");

            stmt.setString(1, uniqueTestName);
            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(dateTimeInDaylightSavingDay);
            stmt.setTimestamp(2, new java.sql.Timestamp(date.getTime()));

            stmt.executeUpdate();
            stmt.close();

        }
    }

    @Override
    protected void tearDown() throws SQLException {
        TestUtil.dropTable(_conn, "dls1test");
        TestUtil.closeDB(_conn);
    }

    public void testDateTimeInDaylightSavingDayFromTable() throws SQLException {
        String sql = "SELECT val, val::text AS val_text FROM dls1test WHERE name = ?";
        PreparedStatement stmt = _conn.prepareStatement(sql);

        for (int i = 1; i <= 20; i++) {
            stmt.setString(1, uniqueTestName);

            ResultSet rs = stmt.executeQuery();
            try {
                if (rs.next()) {

                    java.sql.Timestamp value;

                    value = rs.getTimestamp("val");
                    String text = rs.getString("val_text");
                    //value = rs.getTimestamp("val", Calendar.getInstance()); // <-- this changes nothing

                    Calendar c = Calendar.getInstance();
                    c.setTime(value);

                    //System.out.println(value.getTimezoneOffset()); // this prints the same value
                    // System.out.println(rs.getMetaData().getColumnType(1));

                    assertEquals("try# " + i + " to verify hour of Timestamp " + value + "; where val::text " + text, testHour, c.get(Calendar.HOUR_OF_DAY));

                } else {
                    throw new Error();
                }
            } finally {
                rs.close();
            }
        }
    }

    public void testDateTimeInDaylightSavingDayFromTableAsParameter() throws SQLException {
        String sql = "SELECT name FROM dls1test WHERE val = ?";
        PreparedStatement stmt = _conn.prepareStatement(sql);

        for (int i = 1; i <= 20; i++) {
            stmt.setTimestamp(1, new java.sql.Timestamp(date.getTime()));

            ResultSet rs = stmt.executeQuery();

            try {
                if (rs.next()) {

                    String testName = rs.getString("name");

                    assertEquals("try# " + i + " to verify retrieving name for test " + testName + "; for dateTime " + dateTimeInDaylightSavingDay, testName,
                            uniqueTestName);

                } else {
                    throw new Error();
                }
            } finally {
                rs.close();
            }
        }
    }

    public void testDateTimeInSaylightSavingDayFromNoTable() throws SQLException {
        String sql = "SELECT ('" + dateTimeInDaylightSavingDay + "'::timestamp) as val";

        PreparedStatement stmt = _conn.prepareStatement(sql);

        for (int i = 1; i <= 20; i++) {
            ResultSet rs = stmt.executeQuery();
            try {
                if (rs.next()) {

                    java.sql.Timestamp value;

                    value = rs.getTimestamp("val");

                    Calendar c = Calendar.getInstance();
                    c.setTime(value);

                    assertEquals("try# " + i + " to verify hour of Timestamp " + value, testHour, c.get(Calendar.HOUR_OF_DAY));

                } else {
                    throw new Error();
                }
            } finally {
                rs.close();
            }
        }
    }
}
