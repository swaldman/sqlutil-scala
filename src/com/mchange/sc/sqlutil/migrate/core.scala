package com.mchange.sc.sqlutil.migrate

trait Schema:
  def Version : Int

enum MetadataKey:
  case SchemaVersion
  case CreatorAppVersion
