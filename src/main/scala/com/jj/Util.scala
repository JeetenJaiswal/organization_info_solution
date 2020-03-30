package com.jj

import scala.util.Random.nextInt


object Util {
  def getRandomCTC(minCTC:Int, maxCTC:Int):Int = minCTC+nextInt(maxCTC-minCTC+1)

  def getCTCComponent(ctc:Int,percentage:Int):Float = ctc*percentage/100f

  def getRandomAge(minAge:Int, maxAge:Int):Int = minAge+nextInt(maxAge-minAge+1)

  def getRandomLastNameList:String= {
    val charactersListForLastName = "abcdefghijklmnopqrstuvwxyz"
    randomStringFromCharList(4 + nextInt(10), charactersListForLastName)
  }

  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = nextInt(chars.length)
      if(i == 1)
        sb.append(chars(randomNum).toUpper)
      else
        sb.append(chars(randomNum))
    }
    sb.toString
  }

}