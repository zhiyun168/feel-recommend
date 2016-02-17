package com.feel.utils

import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar}

/**
  * Created by canoe on 1/13/16.
  */
object TimeIssues {

 def nDaysAgoTs(n: Int) = {
   TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
   val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -n)
    dateFormat.parse(dateFormat.format(calendar.getTime)).getTime / 1000
  }

  def nDaysAgoDate(n: Int) = {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -n)
    dateFormat.format(calendar.getTime)
  }

  def tsToDate(ts: Long) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(ts)
  }
}
