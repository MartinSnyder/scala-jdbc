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

object Example {
  val connectionInfo = new Jdbc.ConnectionInfo("jdbc:h2:mem:test1;DB_CLOSE_DELAY=-1", "", "")

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

  def queryToJSON(conn: Jdbc.ConnectionInfo, sql: String) = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    Jdbc.withResultsIterator(conn, sql, (it) => {
      val jsonIt = it.drop(1).take(3).map(m => mapper.writeValueAsString(m))

      jsonIt.mkString("[\n\t", ",\n\t", "\n]")
    })
  }

  def main(args: Array[String]) {
    initializeDatabase()

    queryToJSON(connectionInfo, "SELECT ID, DESCRIPTION FROM EXAMPLE ORDER BY DESCRIPTION DESC") match {
      case Success(json) => println(json)
      case Failure(e) => println(e.getMessage)
    }
  }
}
