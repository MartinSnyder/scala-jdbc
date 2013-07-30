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

package net.martinsnyder.scalajdbc

import java.sql._
import scala.util.Try
import scala.collection.mutable.ListBuffer

/**
 * Container object for Functional wrapper methods for JDBC.
 */
object Jdbc {

  /**
   * Raw structure for holding any and all fields necessary to create a JDBC connection.  Advanced users will
   * likely need to expand on this.
   * @param url JDBC connection URL.  e.g. "jdbc:h2:mem:test1"
   * @param username username for associated connection URL.  Can be null or ""
   * @param password password for associated connection URL.  Can be null or ""
   */
  case class ConnectionInfo(url: String, username: String, password: String) {}

  /**
   * Invokes the supplied function parameter with a properly created and managed JDBC Connection
   * @param connInfo payload to instantiate the JDBC connection
   * @param f function to be invoked using the managed connection
   * @tparam T return type of f.  Can be any type, including Unit
   * @return returns a Try Monad for the operation.  On success, will be Success[T], on failure will be Failure[Exception]
   */
  def withConnection [T] (connInfo: ConnectionInfo, f: (Connection) => T): Try[T] = {
    // Create the JDBC connection
    val conn: Connection = DriverManager.getConnection(connInfo.url, connInfo.username, connInfo.password)

    // Invoke our supplied function inside the Try monad
    val result: Try[T] = Try(f(conn))

    // The Try monad will contain all possible exceptions for us safely, so closing the connection here is
    // akin to a finally block
    conn.close()

    // Return the result of our Try
    result
  }

  /**
   * Invokes the supplied function parameter with a properly created and managed JDBC statement
   *
   * @param connInfo payload to instantiate the JDBC connection
   * @param f function to be invoked using the managed statement
   * @tparam T return type of f.  Can be any type, including Unit
   * @return returns a Try Monad for the operation.  On success, will be Success[T], on failure will be Failure[Exception]
   */
  def withStatement [T] (connInfo: ConnectionInfo, f: (Statement) => T): Try[T] = {
    // Create a private function to pass to withConnection below that implements the logic of this method
    // Note that privFun closes on f, so it is able to invoke our specified function even though privFun
    // will actually be executed by withConnection below
    def privFun(conn: Connection): T = {
      val stmt: Statement = conn.createStatement()

      // We do not need to wrap this in a Try Monad because we know we will be executing inside 'withConnection'
      // which does it for us.  Using another Try(...) here would just create a confusing second layer of structures
      // for the caller to sort through
      try {
        f(stmt)
      }
      finally {
        stmt.close()
      }
    }

    withConnection(connInfo, privFun)
  }

  /**
   * Invokes the supplied function parameter with a properly created and managed JDBC result set
   *
   * @param connInfo payload to instantiate the JDBC connection
   * @param sql SQL Query to execute and bind to the requested result set
   * @param f function to be invoked using the managed result set
   * @tparam T return type of f.  Can be any type, including Unit
   * @return returns a Try Monad for the operation.  On success, will be Success[T], on failure will be Failure[Exception]
   */
  def withResultSet [T] (connInfo: ConnectionInfo, sql: String, f: (ResultSet) => T): Try[T] = {
    // Create a private function to pass to withStatement below that implements the logic of this method
    // Note that privFun closes on f, so it is able to invoke our specified function even though privFun
    // will actually be executed by withStatement below
    def privFun(stmt: Statement): T = {
      val resultSet: ResultSet = stmt.executeQuery(sql)

      // We do not need to wrap this in a Try Monad because we know we will be executing inside 'withConnection'
      // which does it for us.  Using another Try(...) here would just create a confusing second layer of structures
      // for the caller to sort through
      try {
        f(resultSet)
      }
      finally {
        resultSet.close()
      }
    }

    withStatement(connInfo, privFun)
  }

  /**
   * A private class that implements the Scala iterator interface for our JDBC results.
   * This iterates over a Map of String->AnyRef (String->Object in Java terms) and enables
   * Scala collections support directly on the JDBC ResultSet.
   *
   * Note that the lifetime of the Iterator object must be no longer than the lifetime of
   * this ResultSet object.  This class makes no attempt to manage or close the associated
   * JDBC result set.
   *
   * @param resultSet The JDBC ResultSet object to project as an iterator.
   */
  private class ResultsIterator (resultSet: ResultSet) extends Iterator[Map[String, AnyRef]] {
    /**
     * A member variable that is initialized when this object is constructed by examining
     * the metadata associated with the result set and generating a Scala list of associated column
     * names
     */
    private val columnNames: List[String] = {
      val rsmd: ResultSetMetaData = resultSet.getMetaData

      // Note the use of a ListBuffer here.  This is a reasonable thing to do even in a functional
      // language so long as you don't "leak" the mutable object outside of it's original scope.
      val columnNamesBuf = new ListBuffer[String]
      for (i <- 1 to rsmd.getColumnCount) {           // JDBC column indices are 1-based
        columnNamesBuf.append(rsmd.getColumnName(i))
      }

      // Convert the mutable ListBuffer to a immutable List as we return it out of the expression
      columnNamesBuf.toList
    }

    /**
     * Produces a Scala Map containing the Name->Value mappings of the current row data for the result set
     * @param resultSet JDBC ResultSet used to extract current row data
     * @return Scala immutable map containing row data of the ResultSet
     */
    private def buildRowMap(resultSet: ResultSet): Map[String, AnyRef] = {
      val rowMap = new scala.collection.mutable.HashMap[String, AnyRef]

      // Use our already-prepared list of column names to extract the values into the map in a single expression
      columnNames.foreach((columnName: String) => {
        rowMap.put(columnName, resultSet.getObject(columnName))
      })

      // Convert our mutable HashMap to a immutable Map on return so as not to leak our mutable structure to our callers
      rowMap.toMap
    }

    /**
     * Retreives the next row of data from a result set.  Note that this method returns an Option monad
     * If the end of the result set has been reached, it will return None, otherwise it will return Some[Map[String, AnyRef]]
     *
     * @param resultSet JDBC ResultSet to extract row data from
     * @return Some[Map] if there is more row data, or None if the end of the resultSet has been reached
     */
    private def getNextRow(resultSet: ResultSet): Option[Map[String, AnyRef]] = {
      if (resultSet.next())
        Some(buildRowMap(resultSet))
      else
        None
    }

    /**
     * Member variable containing the next row.  We need to manage this state ourselves to defend against implementation
     * changes in how Scala iterators are used.  In particular, we do this to prevent attaching the Scala hasNext function
     * to the ResultSet.next method, which seems generally unsafe.
     */
    private var nextRow = getNextRow(resultSet)

    /**
     * Scala Iterator method called to test if we have more JDBC results
     * @return
     */
    override def hasNext = nextRow.isDefined

    /**
     * Scala Iterator method called to retrieve the next JDBC result
     * @return
     */
    override def next() = {
      // Extract the raw Map out of our Option[Map].  This is generally unsafe to do without risking an exception
      // but no one should be calling next without first making sure that hasNext returns true, so in our usage model
      // we should never invoke get on "None"
      val rowData = nextRow.get

      // Prime our nextRow member variable to contain the next row in line for processing
      nextRow = getNextRow(resultSet)

      // Return the original rowData as the result of our expression
      rowData
    }
  }

  /**
   * Applies the supplied function to a managed Scala Iterator wrapping a JDBC result set
   * @param connInfo payload to instantiate the JDBC connection
   * @param sql SQL Query to execute and bind to the requested result set
   * @param itFun function to be invoked using the managed result set
   * @tparam T return type of f.  Can be any type, including Unit
   * @return returns a Try Monad for the operation.  On success, will be Success[T], on failure will be Failure[Exception]
   */
  def withResultsIterator [T] (connInfo: ConnectionInfo, sql: String, itFun: (ResultsIterator) => T): Try[T] =
    withResultSet(connInfo, sql, (resultSet: ResultSet) => itFun(new ResultsIterator(resultSet)))
}
