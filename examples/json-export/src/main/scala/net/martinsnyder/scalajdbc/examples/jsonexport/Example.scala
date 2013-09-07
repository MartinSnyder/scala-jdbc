/*
  Copyright (c) 2013, Martin Snyder
  All rights reserved.

  Redistribution and use in source and binary forms, with or without modification, are
  permitted provided that the following conditions are met:

  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer
  in the documentation and/or other materials provided with the distribution.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package net.martinsnyder.scalajdbc.examples.jsonexport

import java.sql.Statement
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.martinsnyder.scalajdbc.Jdbc
import scala.util.{Failure, Success}

/**
 * Uses scala-jdbc and jackson to export a relational table to the console as JSON
 */
object Example {
  /**
   * Standard H2 connection string used for testing
   * Refer to http://www.h2database.com/html/main.html for more info
   */
  val connectionInfo = new Jdbc.ConnectionInfo("jdbc:h2:mem:test1;DB_CLOSE_DELAY=-1", "", "")

  /**
   * This is the same sample DB that is used in the project unit tests.  Because the examples are
   *in a src/main though, they cannot access the initializeDatabase in src/test of the parent project
   */
  def initializeDatabase() {
    Jdbc.withStatement(connectionInfo, (stmt: Statement) => {
      stmt.execute("CREATE TABLE EXAMPLE(ID INT PRIMARY KEY, DESCRIPTION VARCHAR)")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(0, 'Zero')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(1, 'One')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(2, 'Two')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(3, 'Three')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(4, 'Four')")
    })
  }

  // A shared Jackson mapper for JSON conversion
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * This is the heart of the example.  Given connection information and a SQL statement
   * this function returns a String containing JSON of all the records in the table
   * @param conn JDBC Connection information
   * @param sql SQL Statement to execute for JSON results conversion
   * @return a single JSON string containing the exported results
   */
  def queryToJSON(conn: Jdbc.ConnectionInfo, sql: String) = Jdbc.withResultsIterator(conn, sql, it => mapper.writeValueAsString(it))

  def main(args: Array[String]) {
    initializeDatabase()

    // Our main invokes our primary routine, then dispatches I/O
    queryToJSON(connectionInfo, "SELECT * FROM EXAMPLE") match {
      case Success(json) => println(json)
      case Failure(e) => println(e.getMessage)
    }
  }
}
