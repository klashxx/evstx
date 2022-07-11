package com.klashxx.github.st

case class UserEvent(id: String, timestamp: Long, isLast: Boolean, data: String)

case class UserSession(id: String, userEvents: Seq[UserEvent])