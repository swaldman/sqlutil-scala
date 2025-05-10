package com.mchange.sc.sqlutil

import java.sql.SQLException

final class UnexpectedRowCountException( message : String, cause : Throwable = null ) extends SQLException( message, cause )
final class UnexpectedlyEmptyResultSet( message : String, cause : Throwable = null )  extends SQLException( message, cause )
final class NonUniqueRow( message : String, cause : Throwable = null )                extends SQLException( message, cause )



