/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.contrib.utils;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.*;
import java.util.ArrayList;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.replay;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class, AmbariDatabase.class})
public class AmbariMySQLDatabaseTest {

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
        DatabaseMetaData mockDBMetadata = null;
        PowerMock.mockStatic(DriverManager.class);
    }



    @Test
    public void enableConstraints() throws SQLException {
        expect(DriverManager.getConnection(anyObject(), anyObject(), anyObject())).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .atLeastOnce();
        expect(mockStatement.executeUpdate("SET FOREIGN_KEY_CHECKS=1")).andReturn(1).once();

        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockStatement);

        AmbariMySQLDatabase oot = new AmbariMySQLDatabase("foo", "bar", "baz");
        oot.enableConstraints();

    }

    @Test
    public void disableConstraints() throws SQLException {
        expect(DriverManager.getConnection(anyObject(), anyObject(), anyObject())).andReturn(
                mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement)
                .atLeastOnce();
        expect(mockStatement.executeUpdate("SET FOREIGN_KEY_CHECKS=0")).andReturn(1).once();

        replay(DriverManager.class);
        replay(mockConnection);
        replay(mockStatement);

        AmbariMySQLDatabase oot = new AmbariMySQLDatabase("foo", "bar", "baz");
        oot.disableConstraints();
    }


}
