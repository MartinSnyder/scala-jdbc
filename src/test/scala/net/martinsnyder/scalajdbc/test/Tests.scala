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

package net.martinsnyder.scalajdbc.test

import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import net.martinsnyder.scalajdbc.Jdbc
import java.sql.{ResultSet, Connection, Statement}
import org.h2.jdbc.JdbcSQLException
import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class Tests extends FunSpec with BeforeAndAfter {
  val connectionInfo = new Jdbc.ConnectionInfo("jdbc:h2:mem:test1;DB_CLOSE_DELAY=-1", "", "")
  val sql = "SELECT ID, DESCRIPTION FROM EXAMPLE ORDER BY DESCRIPTION"

  before {
    Jdbc.withStatement(connectionInfo, (stmt: Statement) => {
      stmt.execute("CREATE TABLE EXAMPLE(ID INT PRIMARY KEY, DESCRIPTION VARCHAR)")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(0, 'Zero')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(1, 'One')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(2, 'Two')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(3, 'Three')")
      stmt.execute("INSERT INTO EXAMPLE(ID, DESCRIPTION) VALUES(4, 'Four')")
    })
  }

  describe("withConnection") {
    it("provides a valid JDBC Connection") {
      assert(Jdbc.withConnection(connectionInfo, c => {
        assert(c.getMetaData != null)
      }).isSuccess)
    }

    it("projects exceptions properly") {
      assert(Jdbc.withConnection(connectionInfo, c => {
        throw new Exception
      }).isFailure)
    }

    it("invalidates the Connection outside of the provided scope") {
      var conn: Connection = null
      assert(Jdbc.withConnection(connectionInfo, c => {
        conn = c
      }).isSuccess)

      assert(conn != null)

      intercept[JdbcSQLException] {
        conn.getMetaData
      }
    }
  }

  describe("withStatement") {
    it("provides a valid JDBC Statement") {
      assert(Jdbc.withStatement(connectionInfo, s => {
        assert(s.getQueryTimeout == 0 || s.getQueryTimeout != 0)
      }).isSuccess)
    }

    it("projects exceptions properly") {
      assert(Jdbc.withStatement(connectionInfo, s => {
        throw new Exception
      }).isFailure)
    }

    it("invalidates the Statement outside of the provided scope") {
      var stmt: Statement = null
      assert(Jdbc.withStatement(connectionInfo, s => {
        stmt = s
      }).isSuccess)

      assert(stmt != null)

      intercept[JdbcSQLException] {
        stmt.getQueryTimeout
      }
    }
  }

  describe("withResultSet") {
    it("provides a valid JDBC ResultSet") {
      assert(Jdbc.withResultSet(connectionInfo, sql, rs => {
        assert(rs.next())
      }).isSuccess)
    }

    it("projects exceptions properly") {
      assert(Jdbc.withResultSet(connectionInfo, sql + "invalid", _ => Unit).isFailure)
    }

    it("invalidates the ResultSet outside of the provided scope") {
      var resultSet: ResultSet = null
      assert(Jdbc.withResultSet(connectionInfo, sql, rs => {
        resultSet = rs
      }).isSuccess)

      assert(resultSet != null)

      intercept[JdbcSQLException] {
        resultSet.next()
      }
    }
  }

  describe("withResultsIterator") {
    it("produces results in the expected order") {
      val results = new ListBuffer[String]

      assert(Jdbc.withResultsIterator(connectionInfo, sql, it => {
        it.foreach(row => {
          row.get("DESCRIPTION") match {
            case Some(desc) => results.append(desc.toString)
            case None => fail("Null value in database")
          }
        })
      }).isSuccess)

      assert(results == List("Four", "One", "Three", "Two", "Zero"))
    }

    it("projects exceptions properly") {
      assert(Jdbc.withResultsIterator(connectionInfo, sql + "invalid", _ => Unit).isFailure)
    }

    it("invalidates the Iterator outside of the provided scope") {
      var iterator: Iterator[Map[String,AnyRef]] = null
      assert(Jdbc.withResultsIterator(connectionInfo, sql, it => {
        iterator = it
      }).isSuccess)

      assert(iterator != null)

      intercept[JdbcSQLException] {
        iterator.next()
      }
    }

    it("behaves as expected when someone calls next too many times") {
      assert(Jdbc.withResultsIterator(connectionInfo, sql, it => {
        it.foreach(row => Unit)

        intercept[NoSuchElementException] {
          it.next()
        }
      }).isSuccess)
    }
  }
}
