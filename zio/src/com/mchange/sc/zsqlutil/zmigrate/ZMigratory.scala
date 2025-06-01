package com.mchange.sc.zsqlutil.zmigrate

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

import scala.util.Using
import scala.util.matching.Regex

import zio.*
import java.time.Instant
import javax.sql.DataSource
import java.time.temporal.ChronoUnit

import com.mchange.sc.sqlutil.*
import com.mchange.sc.sqlutil.migrate.*
import com.mchange.sc.zsqlutil.*
import com.mchange.sc.zsqlutil.LoggingApi.*

object ZMigratory extends SelfLogging:
  private val DumpTimestampFormatter = java.time.format.DateTimeFormatter.ISO_INSTANT
  trait Postgres[T <: Schema] extends ZMigratory[T]:
    def fetchDbName(conn : Connection) : Task[String] =
      ZIO.attemptBlocking:
        Using.resource(conn.createStatement()): stmt =>
          Using.resource(stmt.executeQuery("SELECT current_database()")): rs =>
            uniqueResult("select-current-database-name", rs)( _.getString(1) )
    def runDump( ds : DataSource, dbName : String, dumpFile : os.Path ) : Task[Unit] =
      ZIO.attemptBlocking:
        val parsedCommand = List("pg_dump", dbName)
        os.proc( parsedCommand ).call( stdout = dumpFile )

trait ZMigratory[T <: Schema]:

  import ZMigratory.logAdapter

  val LatestSchema : T
  def targetDbVersion : Int = LatestSchema.Version

  /**
   * Should be overridden a name with no special characters except - or _,
   * e.g. feedletter-pg
   */
  val AppDbTag : String

  def dumpFileName( timestamp : String ) : String = AppDbTag + "-dump." + timestamp + ".sql"

  def createTimestampExtractingDumpFileRegex() : Regex = (s"""^${AppDbTag}-dump\\.(.+)\\.sql$$""").r

  def getRunningAppVersionIfAvailable() : Option[String]

  def fetchDbName(conn : Connection) : Task[String]
  def fetchDumpDir(conn : Connection) : Task[os.Path]
  def runDump( ds : DataSource, dbName : String, dumpFile : os.Path ) : Task[Unit]
  def upMigrate(ds : DataSource, from : Option[Int]) : Task[Unit]

  /**
    * The initial version of the database should include a "metadata" table which maps String keys to String values.
    * Absence of this table will be taken to mean no version of the schema has been initialized.
    */
  val MetadataTableName : String

  def fetchMetadataValue( conn : Connection, key : MetadataKey )  : Option[String]
  def zfetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] = ZIO.attemptBlocking( fetchMetadataValue( conn, key ) )

  lazy val DumpFileNameRegex = createTimestampExtractingDumpFileRegex()

  def fetchDumpDir( ds : DataSource ) : Task[os.Path] = withConnectionZIO(ds)( fetchDumpDir )

  def extractTimestampFromDumpFileName( dfn : String ) : Option[Instant] =
    DumpFileNameRegex.findFirstMatchIn(dfn)
      .map( m => m.group(1) )
      .map( ZMigratory.DumpTimestampFormatter.parse )
      .map( Instant.from )

  def prepareDumpFileForInstant( dumpDir : os.Path, instant : Instant ) : Task[os.Path] = ZIO.attemptBlocking:
    if !os.exists( dumpDir ) then os.makeDir.all( dumpDir )
    val ts = ZMigratory.DumpTimestampFormatter.format( instant )
    dumpDir / dumpFileName(ts)

  def lastHourDumpFileExists( dumpDir : os.Path ) : Task[Boolean] = ZIO.attemptBlocking:
    if os.exists( dumpDir ) then
      val instants =
        os.list( dumpDir )
          .map( _.last )
          .map( extractTimestampFromDumpFileName )
          .collect { case Some(instant) => instant }
      val now = Instant.now
      val anHourAgo = now.minus(1, ChronoUnit.HOURS)
      instants.exists( i => anHourAgo.compareTo(i) < 0 )
    else
      false

  def dump(ds : DataSource) : Task[os.Path] =
    withConnectionZIO( ds ): conn =>
      for
        dbName   <- fetchDbName(conn)
        dumpDir  <- fetchDumpDir(conn)
        dumpFile <- prepareDumpFileForInstant(dumpDir, java.time.Instant.now)
        _        <- runDump( ds, dbName, dumpFile )
      yield dumpFile

  def dbVersionStatus(ds : DataSource) : Task[DbVersionStatus] =
    withConnectionZIO( ds ): conn =>
      val okeyDokeyIsh =
        for
          mbDbVersion <- zfetchMetadataValue(conn, MetadataKey.SchemaVersion)
          mbCreatorAppVersion <- zfetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
        yield
          try
            mbDbVersion.map( _.toInt ) match
              case Some( version ) if version == LatestSchema.Version => DbVersionStatus.Current( version )
              case Some( version ) if version < LatestSchema.Version => DbVersionStatus.OutOfDate( version, LatestSchema.Version )
              case Some( version ) => DbVersionStatus.UnexpectedVersion( Some(version.toString), mbCreatorAppVersion, getRunningAppVersionIfAvailable(), Some(LatestSchema.Version.toString()) )
              case None => DbVersionStatus.SchemaMetadataDisordered( s"Expected key '${MetadataKey.SchemaVersion}' was not found in schema metadata!" )
          catch
            case nfe : NumberFormatException =>
              DbVersionStatus.UnexpectedVersion( mbDbVersion, mbCreatorAppVersion, getRunningAppVersionIfAvailable(), Some(LatestSchema.Version.toString()) )
      okeyDokeyIsh.catchSome:
        case sqle : SQLException =>
          val dbmd = conn.getMetaData()
          try
            val rs = dbmd.getTables(null,null,MetadataTableName,null)
            if !rs.next() then // the metadata table does not exist
              ZIO.succeed( DbVersionStatus.SchemaMetadataNotFound )
            else
              ZIO.succeed( DbVersionStatus.SchemaMetadataDisordered(s"Metadata table found, but an Exception occurred while accessing it: ${sqle.toString()}") )
          catch
            case t : SQLException =>
              WARNING.log("Exception while connecting to database.", t)
              ZIO.succeed( DbVersionStatus.ConnectionFailed )
  end dbVersionStatus

  def migrate(ds : DataSource) : Task[Unit] =
    def handleStatus( status : DbVersionStatus ) : Task[Unit] =
      TRACE.log( s"handleStatus( ${status} )" )
      status match
        case DbVersionStatus.Current(version) =>
          INFO.log( s"Schema up-to-date (current version: ${version})" )
          ZIO.succeed( () )
        case DbVersionStatus.OutOfDate( schemaVersion, requiredVersion ) =>
          assert( schemaVersion < requiredVersion, s"An out-of-date scheme should have schema version (${schemaVersion}) < required version (${requiredVersion})" )
          INFO.log( s"Up-migrating from schema version ${schemaVersion})" )
          upMigrate( ds, Some( schemaVersion ) ) *> migrate( ds )
        case DbVersionStatus.SchemaMetadataNotFound => // uninitialized db, we presume
          INFO.log( s"Initializing new schema")
          upMigrate( ds, None ) *> migrate( ds )
        case other =>
          throw new CannotUpMigrate( s"""${other}: ${other.errMessage.getOrElse("<no message>")}""" ) // we should never see <no message>
    for
      status <- dbVersionStatus( ds )
      _      <- handleStatus( status )
    yield ()

  def cautiousMigrate( ds : DataSource ) : Task[Unit] =
    val safeToTry =
      for
        initialStatus <- dbVersionStatus(ds)
        dumpDir       <- fetchDumpDir(ds)
        dumped        <- lastHourDumpFileExists(dumpDir)
      yield
        initialStatus == DbVersionStatus.SchemaMetadataNotFound || dumped

    safeToTry.flatMap: safe =>
      if safe then
        migrate(ds)
      else
        ZIO.fail( new NoRecentDump("Please dump the database prior to migrating, no recent dump file found.") )

