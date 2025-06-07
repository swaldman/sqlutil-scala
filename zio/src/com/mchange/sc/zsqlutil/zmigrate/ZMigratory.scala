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
    override def fetchDbName(conn : Connection) : Task[Option[String]] =
      ZIO.attemptBlocking:
        Using.resource(conn.createStatement()): stmt =>
          Using.resource(stmt.executeQuery("SELECT current_database()")): rs =>
            uniqueResult("select-current-database-name", rs)( rs => Some( rs.getString(1) ) )

    // see https://stackoverflow.com/questions/29648309/pg-dump-postgres-database-from-remote-server-when-port-5432-is-blocked
    // for some variations in runDump(...) with postgres
    def simpleLocalRunDump( ds : DataSource, mbDbName : Option[String], dumpFile : os.Path ) : Task[Unit] =
      mbDbName match
        case Some( dbName ) =>
          ZIO.attemptBlocking:
            val parsedCommand = List("pg_dump", dbName)
            os.proc( parsedCommand ).call( stdout = dumpFile )
        case None =>
          throw new CannotDump(s"Cannot determine dbName, unable to dump the database without the dbname.")

trait ZMigratory[T <: Schema]:

  import ZMigratory.logAdapter

  /**
   * Should be overridden a name with no special characters except - or _,
   * e.g. feedletter-pg
   */
  val DumpFileAppDbTag : String

  val LatestSchema : T

  def getRunningAppVersionIfAvailable() : Option[String]

  def fetchDbName(conn : Connection) : Task[Option[String]]
  def fetchDumpDir(conn : Connection) : Task[Option[os.Path]]
  def runDump( ds : DataSource, mbDbName : Option[String], dumpFile : os.Path ) : Task[Unit]
  def upMigrate(ds : DataSource, from : Option[Int]) : Task[Unit]

  /**
    * The initial version of the database should include a "metadata" table which maps String keys to String values.
    * Absence of this table will be taken to mean no version of the schema has been initialized.
    */
  val MetadataTableName : String
  def fetchMetadataValue( conn : Connection, key : MetadataKey )             : Option[String]
  def insertMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit
  def updateMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit
  def hasMetadataTable( conn : Connection ) : Boolean

  def dumpFileName( timestamp : String ) : String = DumpFileAppDbTag + "-dump." + timestamp + ".sql"

  def createTimestampExtractingDumpFileRegex() : Regex = (s"""^${DumpFileAppDbTag}-dump\\.(.+)\\.sql$$""").r

  def zfetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] = ZIO.attemptBlocking( fetchMetadataValue( conn, key ) )

  lazy val DumpFileNameRegex = createTimestampExtractingDumpFileRegex()

  def fetchDumpDir( ds : DataSource ) : Task[Option[os.Path]] = withConnectionZIO(ds)( fetchDumpDir )

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

  private def unmaybeDumpDir( mbDumpDir : Option[os.Path], failure : =>Throwable ) : Task[os.Path] =
    mbDumpDir.fold( ZIO.fail( failure ) )( path => ZIO.succeed( path ) )

  def dump(ds : DataSource) : Task[os.Path] =
    withConnectionZIO( ds ): conn =>
      for
        mbDbName  <- fetchDbName(conn)
        mbDumpDir <- fetchDumpDir(conn)
        dumpDir   <- unmaybeDumpDir( mbDumpDir, new CannotDump("Unable to find dump directory in configuration.") )
        dumpFile  <- prepareDumpFileForInstant(dumpDir, java.time.Instant.now)
        _         <- runDump( ds, mbDbName, dumpFile )
      yield dumpFile

  def ensureMetadataTable( conn : Connection ) : Task[Unit] =
    ZIO.attemptBlocking:
      if !hasMetadataTable(conn) then throw new DbNotInitialized("Please initialize the database. (No metadata table found.)")

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

  def ensureDb( ds : DataSource ) : Task[Unit] =
    withConnectionZIO( ds ): conn =>
      for
        _ <- ensureMetadataTable(conn)
        mbSchemaVersion <- zfetchMetadataValue(conn, MetadataKey.SchemaVersion).map( option => option.map( _.toInt ) )
        mbAppVersion <- zfetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
      yield
        mbSchemaVersion match
          case Some( schemaVersion ) =>
            if schemaVersion > LatestSchema.Version then
              throw new MoreRecentApplicationVersionRequired(
                s"The database schema version is ${schemaVersion}. " +
                mbAppVersion.fold("")( appVersion => s"It was created by app version '${appVersion}'. " ) +
                s"The latest version known by this version of the app is ${LatestSchema.Version}. " +
                s"You are running app version '${BuildInfo.version}'."
              )
            else if schemaVersion < LatestSchema.Version then
              throw new SchemaMigrationRequired(
                s"The database schema version is ${schemaVersion}. " +
                mbAppVersion.fold("")( appVersion => s"It was created by app version '${appVersion}'. " ) +
                s"The current schema this version of the app (${BuildInfo.version}) is ${LatestSchema.Version}. " +
                "Please migrate."
              )
            // else schemaVersion == LatestSchema.version and we're good
          case None =>
            throw new DbNotInitialized("Please initialize the database.")
  end ensureDb

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
        mbDumpDir     <- fetchDumpDir(ds)
        dumpDir       <- unmaybeDumpDir( mbDumpDir, new CannotUpMigrate("Unable to find dump directory to verify existence of a recent dump prior to cautious upmigration." ) )
        dumped        <- lastHourDumpFileExists(dumpDir)
      yield
        initialStatus == DbVersionStatus.SchemaMetadataNotFound || dumped

    safeToTry.flatMap: safe =>
      if safe then
        migrate(ds)
      else
        ZIO.fail( new NoRecentDump("Please dump the database prior to migrating, no recent dump file found.") )

