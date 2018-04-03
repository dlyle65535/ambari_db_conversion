/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.contrib.utils;


import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import java.sql.*;
import java.util.ArrayList;

import static org.powermock.api.easymock.PowerMock.*;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class,MysqlDB.class})
public class MysqlDBTest {

    Connection mockConnection = null;
    Statement mockStatement = null;
    ResultSet mockResultSet = null;


    @Before
    public void setUp() throws Exception {
        mockConnection = createMock(Connection.class);
        mockStatement = createMock(Statement.class);
        mockResultSet = createMock(ResultSet.class);
        PowerMock.mockStatic(DriverManager.class);
    }


    @Test
    public void deleteFromTable() throws SQLException {
        expect(DriverManager.getConnection(anyObject(),anyObject(),anyObject())).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .atLeastOnce();
        expect(mockStatement.executeUpdate("SET FOREIGN_KEY_CHECKS=0")).andReturn(1).once();
        expect(mockStatement.executeUpdate("TRUNCATE TABLE " + "tablename")).andReturn(1).once();

        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockStatement);


        MysqlDB oot = new MysqlDB("foo","bar","baz");
        oot.deleteFromTable("tablename");

    }

    @Test
    public void normalizeTableNames(){

        assertEquals("qrtz tables should be capitalized","QRTZ_TABLE_NAME",MysqlDB.normalizeTablename("qrtz_table_name"));
        assertEquals("clusterhostmapping should be CamelCased","ClusterHostMapping",MysqlDB.normalizeTablename("clusterhostmapping"));

    }

    @Test
    public void buildInsertStatement() throws SQLException {
        expect(DriverManager.getConnection(anyObject(),anyObject(),anyObject())).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .atLeastOnce();
        expect(mockStatement.executeUpdate("SET FOREIGN_KEY_CHECKS=0")).andReturn(1).once();
        expect(mockConnection.prepareStatement("INSERT INTO tablename(first, second, third) VALUES (?, ?, ?)")).andReturn(null).once();

        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockStatement);

        MysqlDB oot = new MysqlDB("foo","bar","baz");
        PreparedStatement ps = oot.buildInsertStatement("tablename",new ArrayList<String>(){{add("first"); add("second"); add("third");}});


    }

    @Test
    public void enableConstraints() throws SQLException {
        expect(DriverManager.getConnection(anyObject(),anyObject(),anyObject())).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .atLeastOnce();
        expect(mockStatement.executeUpdate("SET FOREIGN_KEY_CHECKS=1")).andReturn(1).once();

        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockStatement);

        MysqlDB oot = new MysqlDB("foo","bar","baz");
        oot.enableConstraints();

    }

    @Test
    public void disableConstraints() throws SQLException {
        expect(DriverManager.getConnection(anyObject(),anyObject(),anyObject())).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .atLeastOnce();
        expect(mockStatement.executeUpdate("SET FOREIGN_KEY_CHECKS=0")).andReturn(1).once();

        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockStatement);

        MysqlDB oot = new MysqlDB("foo","bar","baz");
        oot.disableConstraints();
    }
}

/**
 import static org.easymock.EasyMock.expect;
 import static org.mockito.Mockito.mock;
 import static org.mockito.Mockito.verify;
 import static org.mockito.Mockito.when;
 import static org.powermock.api.easymock.PowerMock.mockStatic;
 import static org.powermock.api.easymock.PowerMock.replay;


 @RunWith(PowerMockRunner.class)
 @PrepareForTest({DriverManager.class, H2Persistence.class})
 public class H2PersistenceTest {
 @Test
 public void testDropPersonIsCalled() throws SQLException {
 final Statement statement = mock(Statement.class);

 final Connection connection = mock(Connection.class);

 when(connection.createStatement()).thenReturn(statement);

 mockStatic(DriverManager.class);

 expect(DriverManager.getConnection(H2Persistence.CONN_TYPE_USER_HOME))
 .andReturn(connection);
 expect(DriverManager.getConnection(null))
 .andReturn(null);

 replay(DriverManager.class);
 final H2Persistence objectUnderTest = new H2Persistence();

 objectUnderTest.open();

 verify(statement).executeUpdate("DROP TABLE IF EXISTS PERSON");
 verify(statement).executeUpdate(H2Persistence.CREATE_TABLE_PERSON);
 }
 }
 **/