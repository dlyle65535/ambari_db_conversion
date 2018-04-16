package org.apache.ambari.contrib.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class AmbariDBConvertTest {

    @Test
    public void convert() {
        /**
         * convert()
         * disable mysql constraints
         * get psql tables
         * for each table
         *    (skip key_value_store)
         *    get rows
         *    get row metadata
         *    get row column count (21 column types covered)
         *    build insert statement
         *    for each row
         *      for each column
         *          map types into insert statement
         *      call execute update
         *
         *
         */
    }
}