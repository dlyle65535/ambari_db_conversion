package org.apache.ambari.contrib.utils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.*;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.replay;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class, PostgresDB.class})
public class PostgresDBTest {

    Connection mockConnection = null;
    Statement mockStatement = null;
    ResultSet mockResultSet = null;
    DatabaseMetaData mockDBMetadata = null;

    @Before
    public void setUp() throws Exception {
        mockConnection = createMock(Connection.class);
        mockStatement = createMock(Statement.class);
        mockResultSet = createMock(ResultSet.class);
        mockDBMetadata = createMock(DatabaseMetaData.class);
        PowerMock.mockStatic(DriverManager.class);
    }

    @Test
    public void getTables() throws SQLException {
        expect(DriverManager.getConnection(anyObject(String.class), anyObject(String.class), anyObject(String.class))).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .once();
        expect(mockConnection.getMetaData()).andReturn(mockDBMetadata).once();
        expect(mockDBMetadata.getTables(null, null, "%", PostgresDB.TABLE_TYPES)).andReturn(mockResultSet).once();
        expect(mockResultSet.next()).andReturn(false).once();
        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockResultSet);
        replay(mockDBMetadata);
        PostgresDB oot = new PostgresDB("foo", "bar", "baz");
        oot.getTables();


    }

    @Test
    public void getRows() throws SQLException {
        expect(DriverManager.getConnection(anyObject(String.class), anyObject(String.class), anyObject(String.class))).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .atLeastOnce();
        expect(mockStatement.executeQuery("SELECT * FROM table;")).andReturn(mockResultSet).once();

        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockStatement);
        replay(mockResultSet);

        PostgresDB oot = new PostgresDB("foo", "bar", "baz");
        oot.getRows("table");

    }
}
