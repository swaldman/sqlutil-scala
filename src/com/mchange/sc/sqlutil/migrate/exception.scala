package com.mchange.sc.sqlutil.migrate

import java.sql.SQLException

class DbMigrationException(msg : String, cause : Throwable = null ) extends SQLException(msg, cause)

class CannotUpMigrate(msg : String, cause : Throwable = null )                      extends DbMigrationException(msg, cause)
class NoRecentDump( msg : String, cause : Throwable = null )                        extends DbMigrationException(msg, cause)
class DbNotInitialized( msg : String, cause : Throwable = null )                    extends DbMigrationException(msg, cause)
class SchemaMigrationRequired( msg : String, cause : Throwable = null )             extends DbMigrationException(msg, cause)
