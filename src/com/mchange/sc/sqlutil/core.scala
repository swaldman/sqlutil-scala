package com.mchange.sc.sqlutil

import java.sql.{Connection,PreparedStatement,ResultSet,Statement,Timestamp,Types}

import scala.util.Using
import scala.util.control.NonFatal

trait Creatable:
  protected def Create : String
  def create( stmt : Statement ) : Int = stmt.executeUpdate( this.Create )
  def create( conn : Connection ) : Int = Using.resource( conn.createStatement() )( stmt => create(stmt) )

def transact[T]( conn : Connection )( block : Connection => T ) : T =
  val origAutoCommit = conn.getAutoCommit()
  try
    conn.setAutoCommit(false)
    val out = block( conn )
    conn.commit()
    out
  catch
    case NonFatal( t ) =>
      conn.rollback()
      throw t
  finally
    conn.setAutoCommit( origAutoCommit )

def uniqueResult[T]( queryDesc : String, rs : ResultSet )( materialize : ResultSet => T ) : T =
  if !rs.next() then
    throw new UnexpectedlyEmptyResultSet(s"Expected a value for ${queryDesc}, none found.")
  else
    val out = materialize(rs)
    if rs.next() then
      throw new NonUniqueRow(s"Expected a unique value for ${queryDesc}. Multiple rows found.")
    else
      out

def zeroOrOneResult[T]( queryDesc : String, rs : ResultSet )( materialize : ResultSet => T ) : Option[T] =
  if !rs.next() then
    None
  else
    val out = materialize(rs)
    if rs.next() then
      throw new NonUniqueRow(s"Expected a unique value for ${queryDesc}. Multiple rows found.")
    else
      Some(out)

private def getSingleValue[T]( extractor : ResultSet => T)( rs : ResultSet ) : T = uniqueResult("query", rs)(extractor)

def getSingleString( rs : ResultSet )  = getSingleValue( _.getString(1) )( rs )
def getSingleBoolean( rs : ResultSet ) = getSingleValue( _.getBoolean(1) )( rs )
def getSingleInt( rs : ResultSet )     = getSingleValue( _.getInt(1) )( rs )
def getSingleLong( rs : ResultSet )    = getSingleValue( _.getLong(1) )( rs )
def getSingleFloat( rs : ResultSet )   = getSingleValue( _.getFloat(1) )( rs )
def getSingleDouble( rs : ResultSet )  = getSingleValue( _.getDouble(1) )( rs )

private def getMaybeSingleValue[T]( extractor : ResultSet => T)( rs : ResultSet ) : Option[T] = zeroOrOneResult("query", rs)( extractor )

def getMaybeSingleString( rs : ResultSet )  = getMaybeSingleValue( _.getString(1) )( rs )
def getMaybeSingleBoolean( rs : ResultSet ) = getMaybeSingleValue( _.getBoolean(1) )( rs )
def getMaybeSingleInt( rs : ResultSet )     = getMaybeSingleValue( _.getInt(1) )( rs )
def getMaybeSingleLong( rs : ResultSet )    = getMaybeSingleValue( _.getLong(1) )( rs )
def getMaybeSingleFloat( rs : ResultSet )   = getMaybeSingleValue( _.getFloat(1) )( rs )
def getMaybeSingleDouble( rs : ResultSet )  = getMaybeSingleValue( _.getDouble(1) )( rs )

def setMaybeString( sqlType : Int )( ps : PreparedStatement, index : Int, mbValue : Option[String] )  : Unit = {
  mbValue.fold( ps.setNull( index, sqlType ) )( value => ps.setString( index, value ) )
}


def setStringOptional( ps : PreparedStatement, position : Int, sqlType : Int, value : Option[String] ) =
  value match
    case Some( s ) => ps.setString(position, s)
    case None      => ps.setNull( position, sqlType )

def setTimestampOptional( ps : PreparedStatement, position : Int, value : Option[Timestamp] ) =
  value match
    case Some( ts ) => ps.setTimestamp(position, ts)
    case None       => ps.setNull( position, Types.TIMESTAMP )

def setLongOptional( ps : PreparedStatement, position : Int, sqlType : Int, value : Option[Long] ) =
  value match
    case Some( l ) => ps.setLong(position, l)
    case None      => ps.setNull( position, sqlType )

def toSet[T]( rs : ResultSet )( extract : ResultSet => T ) : Set[T] =
  val builder = Set.newBuilder[T]
  while rs.next() do
    builder += extract(rs)
  builder.result()

def toSeq[T]( rs : ResultSet )( extract : ResultSet => T ) : Seq[T] =
  val builder = Seq.newBuilder[T]
  while rs.next() do
    builder += extract(rs)
  builder.result()

