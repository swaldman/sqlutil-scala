package com.mchange.sc.sqlutil.migrate

class DbMigrationException(msg : String, cause : Throwable = null ) extends Exception(msg, cause)

class CannotUpMigrate(msg : String, cause : Throwable = null )                       extends DbMigrationException(msg, cause)
class NoRecentDump( msg : String, cause : Throwable = null )                         extends DbMigrationException(msg, cause)
class CannotDump( msg : String, cause : Throwable = null )                           extends DbMigrationException(msg, cause)
class DbNotInitialized( msg : String, cause : Throwable = null )                     extends DbMigrationException(msg, cause)
class SchemaMigrationRequired( msg : String, cause : Throwable = null )              extends DbMigrationException(msg, cause)
class MoreRecentApplicationVersionRequired( msg : String, cause : Throwable = null ) extends DbMigrationException(msg, cause)

