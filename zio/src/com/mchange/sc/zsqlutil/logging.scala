package com.mchange.sc.zsqlutil

object LoggingApi:
  val raw = logadapter.zio.ZApi( logadapter.mlog.Api )
  type SelfLogging = raw.inner.SelfLogging
  export raw.*

