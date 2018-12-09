import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.types.{DateType, FloatType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.{SparkContext, SparkConf}
// For implicit conversions from RDDs to DataFrames

object queries {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Contains SQLContext which is necessary to execute SQL queries
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Reads json file and stores in a variable
    val tweet = sqlContext.read.json("D:\\PB\\tweets_output.txt")
    val outputFile = "C:\\Documents\\outputFile"
    //To register tweets data as a table
    tweet.createOrReplaceTempView("tweets")
    val disCat = sqlContext.sql("SELECT user.name as UserName,user.location as loc,text,created_at," +
      "CASE WHEN text like '%amazon%' THEN 'AMAZON  '" +
      "WHEN text like '%google%' THEN 'GOOGLE'" +
      "WHEN text like '%walmart%' THEN 'WALMART'" +
      "WHEN text like '%apple%' THEN 'APPLE'" +
      "WHEN text like '%yahoo%' THEN 'YAHOO'" +
      "WHEN text like '%at&t%' THEN 'AT&T'" +
      "WHEN text like '%oracle%' THEN 'ORACLE'" +
      "WHEN text like '%tesla%' THEN 'TESLA'" +
      "WHEN text like '%intel%' THEN 'INTEL'" +
      "WHEN text like '%sony%' THEN 'SONY'" +
      "WHEN text like '%uber%' THEN 'UBER'" +
      "END AS brandType from tweets where text is not null")
    disCat.createOrReplaceTempView("disCat2")
    val disCat3 = sqlContext.sql("SELECT user.name as UserName,user.location as loc,text,created_at," +
      "CASE WHEN text like '%Windows%' OR text like '%windows%' THEN 'Windows'" +
      "WHEN text like '%outlook%' OR text like '%Outlook%' THEN 'Outlook'" +
      "WHEN text like '%skype%' OR text like '%Skype%' THEN 'skype'" +
      "WHEN text like '%alexa%' OR text like '%Alexa%' THEN 'Alexa'" +
      "END AS brandType from tweets where text is not null")
    disCat3.createOrReplaceTempView("disCat4")
    println("Enter any one of the following query to get data")
    println("1.Query-1:This query fetches the brands and its popularity based on tweets data")
    println("2.Query-2:Which user tweeted most about which type of brand")
    println("3.Query-3:Tweets from different countries about brands")
    println("4.Query-4:On which day more tweets are done")
    println("5.Query-5:This query fetches Tweets count for different types of phone models")
    println("6.Query-6:Popular languages used for tweeting tweets about brands")
    println("7.Query-7:Number of tweets for particular date ")
    println("8.Query-8:Account Verification tweets")
    println("9.Query-9:On Which hours More Tweets Were Done")
    println("10.Query-10:Which state is mostly having tweets about type of brand")
    println("Enter any one of the following query to get data:")
    val count = scala.io.StdIn.readLine()
    count match {
      case "1" =>
        /*--------------------Query 1: This query fetches the BRANDS and their popularity based on tweets data-----------------------*/
        val textFile = sc.textFile("D:\\PB\\tweets_output.txt")
        val amazon = textFile.filter(line => line.contains("#amazon")).count()
        val google = textFile.filter(line => line.contains("#google")).count()
        val walmart = textFile.filter(line => line.contains("#walmart")).count()
        val apple = textFile.filter(line => line.contains("#apple")).count()
        val yahoo = textFile.filter(line => line.contains("#yahoo")).count()
        val att = textFile.filter(line => line.contains("#at&t")).count()
        val oracle = textFile.filter(line => line.contains("#oracle")).count()
        val tesla = textFile.filter(line => line.contains("#tesla")).count()
        val intel = textFile.filter(line => line.contains("#intel")).count()
        val uber = textFile.filter(line => line.contains("#uber")).count()
        val sony = textFile.filter(line => line.contains("#sony")).count()
        println("********************************************")
        println("Number of tweets on different types of brands")
        println("********************************************")
        println("Amazon : %s".format(amazon))
        println("Google : %s".format(google))
        println("Walmart : %s".format(walmart))
        println("Apple : %s".format(apple))
        println("Yahoo : %s".format(yahoo))
        println("AT&T : %s".format(att))
        println("Oracle : %s".format(oracle))
        println("Tesla : %s".format(tesla))
        println("Intel : %s".format(intel))
        println("Uber : %s".format(uber))
        println("Sony : %s".format(sony))
      /*-----------------------------Query 2:  Which user tweeted most about which type of brand--------------------------------------------*/
      case "2" =>

        val r1 = sqlContext.sql("SELECT UserName,'AMAZON' as brandType,count(*) as count FROM disCat2 WHERE brandType='AMAZON' " +
          "group by UserName order by count desc limit 1")
        val r2 = sqlContext.sql("SELECT UserName,'WALMART' as brandType,count(*) as count FROM disCat2 WHERE brandType='WALMART' " +
          "group by UserName order by count desc limit 1 ")
        val r3 = sqlContext.sql("SELECT UserName,'GOOGLE' as brandType,count(*) as count FROM disCat2 WHERE brandType='GOOGLE' " +
          "group by UserName order by count desc limit 1 ")
        val r4 = sqlContext.sql("SELECT UserName,'APPLE' as brandType,count(*) as count FROM disCat2 WHERE brandType='APPLE' " +
          "group by UserName order by count desc limit 1 ")
        val r5 = sqlContext.sql("SELECT UserName,'YAHOO' as brandType,count(*) as count FROM disCat2 WHERE brandType='YAHOO' " +
          "group by UserName order by count desc limit 1 ")
        val r6 = sqlContext.sql("SELECT UserName,'AT&T' as brandType,count(*) as count FROM disCat2 WHERE brandType='AT&T' " +
          "group by UserName order by count desc limit 1 ")
        val r7 = sqlContext.sql("SELECT UserName,'ORACLE' as brandType,count(*) as count FROM disCat2 WHERE brandType='ORACLE' " +
          "group by UserName order by count desc limit 1 ")
        val r8 = sqlContext.sql("SELECT UserName,'TESLA' as brandType,count(*) as count FROM disCat2 WHERE brandType='TESLA' " +
          "group by UserName order by count desc limit 1 ")
        val r9 = sqlContext.sql("SELECT UserName,'INTEL' as brandType,count(*) as count FROM disCat2 WHERE brandType='INTEL' " +
          "group by UserName order by count desc limit 1")
        val r10 = sqlContext.sql("SELECT UserName,'UBER' as brandType,count(*) as count FROM disCat2 WHERE brandType='UBER' " +
          "group by UserName order by count desc limit 1")
        val r11 = sqlContext.sql("SELECT UserName,'SONY' as brandType,count(*) as count FROM disCat2 WHERE brandType='SONY' " +
          "group by UserName order by count desc limit 1 ")

        val rdd1 = r1.union(r2).union(r3).union(r4).union(r5).union(r6).union(r7).union(r8).union(r9).union(r10).union(r11)

        println("****************************************")
        println("Which user tweeted more on which type of brand")
        println("****************************************")
        rdd1.show()
      /*-----------------------------------Query 3: Tweets from different countries about brands -------------------------------------*/
      case "3" =>
        val countrytweetscount=sqlContext.sql("SELECT distinct place.country, count(*) as count FROM tweets where place.country is not null " + "GROUP BY place.country ORDER BY count DESC")
        countrytweetscount.createOrReplaceTempView("countrytweetscount")
        println("****************************************")
        println("Tweets from different countries")
        println("****************************************")
        countrytweetscount.show()
      /*-------------------------------Query 4 : On which Day More Tweets are posted-----------------------------------*/
      case "4" =>
        val day_data = sqlContext.sql("SELECT substring(user.created_at,1,3) as day from tweets where text is not null")

        day_data.createOrReplaceTempView("day_data")

        val days_final = sqlContext.sql(
          """ SELECT Case
            |when day LIKE '%Mon%' then 'WEEKDAY'
            |when day LIKE '%Tue%' then 'WEEKDAY'
            |when day LIKE '%Wed%' then 'WEEKDAY'
            |when day LIKE '%Thu%' then 'WEEKDAY'
            |when day LIKE '%Fri%' then 'WEEKDAY'
            |when day LIKE '%Sat%' then 'WEEKEND'
            |when day LIKE '%Sun%' then 'WEEKEND'
            | else
            | null
            | end as day1 from day_data where day is not null""".stripMargin)

        days_final.createOrReplaceTempView("days_final")

        val res = sqlContext.sql("SELECT day1 as Day,Count(*) as Day_Count from days_final where day1 is not null group by day1 order by count(*) desc")

        println ("**********************************")
        println("On Which Day More Tweets Were Done")
        println("**********************************")
        res.show()
      /*-----------------------------------Query 5: Tweets count for different types of brand products -------------------------------------*/
      case "5" =>
        val r1 = sqlContext.sql("SELECT loc,'Windows' as brandType,count(*) as count FROM disCat4 WHERE brandType='Windows' " +
          "group by loc order by count desc limit 10")
        val r2 = sqlContext.sql("SELECT loc,'Outlook' as brandType,count(*) as count FROM disCat4 WHERE brandType='Outlook' " +
          "group by loc order by count desc limit 10")
        val r3 = sqlContext.sql("SELECT loc,'skype' as brandType,count(*) as count FROM disCat4 WHERE brandType='skype' " +
          "group by loc order by count desc limit 10")
        val r4 = sqlContext.sql("SELECT loc,'Alexa' as brandType,count(*) as count FROM disCat4 WHERE brandType='Alexa' " +
          "group by loc order by count desc limit 10")

        val rdd1 = r1.union(r2).union(r3).union(r4)
        rdd1.createOrReplaceTempView("rdd1")
        val res=sqlContext.sql("SELECT brandType, Count(*) as Count from rdd1 where brandType is not null group by brandType")
        println("****************************************")
        println("products")
        println("****************************************")
        res.show()
      /*-----------------------------------Query 6 Popular languages used for tweeting tweets about brands -------------------------------------*/
      case "6" =>
        val langWstCount = sqlContext.sql("SELECT distinct id," +
          "CASE when user.lang LIKE '%en%' then 'English'"+
          "when user.lang LIKE '%ja%' then 'Japanese'"+
          "when user.lang LIKE '%es%' then 'Spanish'"+
          "when user.lang LIKE '%fr%' then 'French'"+
          "when user.lang LIKE '%it%' then 'Italian'"+
          "when user.lang LIKE '%ru%' then 'Russian'"+
          "when user.lang LIKE '%ar%' then 'Arabic'"+
          "when user.lang LIKE '%bn%' then 'Bengali'"+
          "when user.lang LIKE '%cs%' then 'Czech'"+
          "when user.lang LIKE '%da%' then 'Danish'"+
          "when user.lang LIKE '%de%' then 'German'"+
          "when user.lang LIKE '%el%' then 'Greek'"+
          "when user.lang LIKE '%fa%' then 'Persian'"+
          "when user.lang LIKE '%fi%' then 'Finnish'"+
          "when user.lang LIKE '%fil%' then 'Filipino'"+
          "when user.lang LIKE '%he%' then 'Hebrew'"+
          "when user.lang LIKE '%hi%' then 'Hindi'"+
          "when user.lang LIKE '%hu%' then 'Hungarian'"+
          "when user.lang LIKE '%id%' then 'Indonesian'"+
          "when user.lang LIKE '%ko%' then 'Korean'"+
          "when user.lang LIKE '%msa%' then 'Malay'"+
          "when user.lang LIKE '%nl%' then 'Dutch'"+
          "when user.lang LIKE '%no%' then 'Norwegian'"+
          "when user.lang LIKE '%pl%' then 'Polish'"+
          "when user.lang LIKE '%pt%' then 'Portuguese'"+
          "when user.lang LIKE '%ro%' then 'Romanian'"+
          "when user.lang LIKE '%sv%' then 'Swedish'"+
          "when user.lang LIKE '%th%' then 'Thai'"+
          "when user.lang LIKE '%tr%' then 'Turkish'"+
          "when user.lang LIKE '%uk%' then 'Ukrainian'"+
          "when user.lang LIKE '%ur%' then 'Urdu'"+
          "when user.lang LIKE '%vi%' then 'Vietnamese'"+
          "when user.lang LIKE '%zh-cn%' then 'Chinese (Simplified)'"+
          "when user.lang LIKE '%zh-tw%' then 'Chinese (Traditional)'"+
          "END AS language from tweets where text is not null")
        langWstCount.createOrReplaceTempView("langWstCount")
        var langWstDataCount=sqlContext.sql("SELECT language, Count(language) as Count from langWstCount where id is NOT NULL and language is not null group by language order by Count DESC")

        println("****************************************")
        println("Language")
        println("****************************************")
        langWstDataCount.show()
      /*-----------------------------------Query 7 number of tweets for particular date  -------------------------------------*/
      case "7" =>
        val tweetcount=sqlContext.sql("SELECT SUBSTR(created_at, 0, 10) tweet_date, COUNT(1) tweet_count FROM   tweets GROUP  BY SUBSTR(created_at, 0, 10) ORDER  BY COUNT(1) DESC LIMIT  5")
        tweetcount.createOrReplaceTempView("tweetcount")
        println("****************************************")
        println("tweet Count")
        println("****************************************")
        tweetcount.show()
      /*-----------------------------------Query 8 Account Verification tweets -------------------------------------*/
      case "8" =>
        val acctVerify=sqlContext.sql("SELECT distinct id, " +
          "CASE when user.verified LIKE '%true%' THEN 'VERIFIED ACCOUNT'"+
          "when user.verified LIKE '%false%' THEN 'NON-VERIFIED ACCOUNT'"+
          "END AS Verified from tweets where text is not null")
        acctVerify.createOrReplaceTempView("acctVerify")
        var acctVerifydata=sqlContext.sql("SELECT  Verified, Count(Verified) as Count from acctVerify where id is NOT NULL and Verified is not null group by Verified order by Count DESC")

        println("****************************************")
        println("Account Verification")
        println("****************************************")
        acctVerifydata.show()
      /*-----------------------------------Query 9 During Which hours More Tweets Were Done -------------------------------------*/
      case "9" =>
        val timehour = sqlContext.sql("SELECT SUBSTRING(created_at,12,2) as hour from tweets where text is not null")

        timehour.createOrReplaceTempView("timehour")

        val timeAnalysis=sqlContext.sql(""" SELECT Case
            |when hour>=0 and hour <4 then 'midnight'
            |when hour>=4 and hour <7 then 'early Morning'
            |when hour>=7 and hour <12 then 'Morning'
            |when hour>=12 and hour <15 then 'afternoon'
            |when hour>=15 and hour <18 then 'evening'
            |when hour>=18 and hour <=23 then 'night'
            end as time from timehour""".stripMargin)

        timeAnalysis.createOrReplaceTempView("timeAnalysis")

        val res = sqlContext.sql("SELECT time as hour,Count(*) as tweets_count from timeAnalysis where time is not null group by time order by count(*) desc")

        println ("**********************************")
        println("On Which hours More Tweets Were Done")
        println("**********************************")
        res.show()
      /*-----------------------------------Query 10 Which state is mostly having tweets about type of brand -------------------------------------*/
      case "10" =>
        val amazonRDD =  sqlContext.sql(""" SELECT 'amazon' as brandType, user.location as loc from tweets where text LIKE '%#amazon%' """)
        val googleRDD =  sqlContext.sql(""" SELECT 'google' as brandType, user.location as loc from tweets where text LIKE '%#google%' """)
        val teslaRDD =  sqlContext.sql(""" SELECT 'tesla' as brandType, user.location as loc from tweets where text LIKE '%#tesla%' """)
        val uberRDD =  sqlContext.sql(""" SELECT 'uber' as brandType, user.location as loc from tweets where text LIKE '%#uber%' """)
        val sql2RDD = amazonRDD.union(googleRDD).union(teslaRDD).union(uberRDD)
        sql2RDD.createOrReplaceTempView("sql2RDD")
        val locate = sqlContext.sql(
          """ SELECT brandType, loc from sql2RDD  where
            |loc LIKE  '%Alaska%' OR loc LIKE  '%Arizona%' OR loc LIKE  '%Arkansas%' OR loc LIKE  '%California%' OR loc LIKE  '%Colorado%' OR loc LIKE  '%Connecticut%' OR loc LIKE  '%Delaware%'
            |OR loc LIKE  '%Florida%'
            |OR loc LIKE  '%Georgia%'
            |OR loc LIKE  '%Hawaii%'
            |OR loc LIKE  '%Idaho%'
            |OR loc LIKE  '%Illinois%'
            |OR loc LIKE  '%Indiana%'
            |OR loc LIKE  '%Iowa%'
            |OR loc LIKE  '%Kansas%'
            |OR loc LIKE  '%Kentucky%'
            |OR loc LIKE  '%Louisiana%'
            |OR loc LIKE  '%Maine%'
            |OR loc LIKE  '%Maryland%'
            |OR loc LIKE  '%Massachusetts%'
            |OR loc LIKE  '%Michigan%'
            |OR loc LIKE  '%Minnesota%'
            |OR loc LIKE  '%Mississippi%'
            |OR loc LIKE  '%Missouri%'
            |OR loc LIKE  '%Montana%'
            |OR loc LIKE  '%Nebraska%'
            |OR loc LIKE  '%Nevada%'
            |OR loc LIKE  '%NewHampshire%'
            |OR loc LIKE  '%NewJersey%'
            |OR loc LIKE  '%NewMexico%'
            |OR loc LIKE  '%NewYork%'
            |OR loc LIKE  '%NorthCarolina%'
            |OR loc LIKE  '%NorthDakota%'
            |OR loc LIKE  '%Ohio%'
            |OR loc LIKE  '%Oklahoma%'
            |OR loc LIKE  '%Oregon%'
            |OR loc LIKE  '%Pennsylvania%'
            |OR loc LIKE  '%RhodeIsland%'
            |OR loc LIKE  '%SouthCarolina%'
            |OR loc LIKE  '%SouthDakota%'
            |OR loc LIKE  '%Tennessee%'
            |OR loc LIKE  '%Texas%'
            |OR loc LIKE  '%Utah%'
            |OR loc LIKE  '%Vermont%'
            |OR loc LIKE  '%Virginia%'
            |OR loc LIKE  '%Washington%'
            |OR loc LIKE  '%WestVirginia%'
            |OR loc LIKE  '%Wisconsin%'
            |OR loc LIKE  '%Wyoming%'
            |OR loc LIKE '% AL%'
            |OR loc LIKE '% AK%'
            |OR loc LIKE '% AZ%'
            |OR loc LIKE '% AR%'
            |OR loc LIKE '% CA%'
            |OR loc LIKE '% CO%'
            |OR loc LIKE '% CT%'
            |OR loc LIKE '% DE%'
            |OR loc LIKE '% FL%'
            |OR loc LIKE '% GA%'
            |OR loc LIKE '% HI%'
            |OR loc LIKE '% ID%'
            |OR loc LIKE '% IL%'
            |OR loc LIKE '% IN%'
            |OR loc LIKE '% IA%'
            |OR loc LIKE '% KS%'
            |OR loc LIKE '% KY%'
            |OR loc LIKE '% LA%'
            |OR loc LIKE '% ME%'
            |OR loc LIKE '% MD%'
            |OR loc LIKE '% MA%'
            |OR loc LIKE '% MI%'
            |OR loc LIKE '% MN%'
            |OR loc LIKE '% MS%'
            |OR loc LIKE '% MO%'
            |OR loc LIKE '% MT%'
            |OR loc LIKE '% NE%'
            |OR loc LIKE '% NV%'
            |OR loc LIKE '% NH%'
            |OR loc LIKE '% NJ%'
            |OR loc LIKE '% NM%'
            |OR loc LIKE '% NY%'
            |OR loc LIKE '% NC%'
            |OR loc LIKE '% ND%'
            |OR loc LIKE '% OH%'
            |OR loc LIKE '% OK%'
            |OR loc LIKE '% OR%'
            |OR loc LIKE '% PA%'
            |OR loc LIKE '% RI%'
            |OR loc LIKE '% SC%'
            |OR loc LIKE '% SD%'
            |OR loc LIKE '% TN%'
            |OR loc LIKE '% TX%'
            |OR loc LIKE '% UT%'
            |OR loc LIKE '% VT%'
            |OR loc LIKE '% VA%'
            |OR loc LIKE '% WA%'
            |OR loc LIKE '% WV%'
            |OR loc LIKE '% WI%'
            |OR loc LIKE '% WY%'
            |""".stripMargin)
        locate.createOrReplaceTempView("locate")

        val sql3RDD =  sqlContext.sql(
          """ SELECT brandType, Case
            |when loc LIKE '%Alaska%' then 'AK'
            |when loc LIKE '%Arizona%' then 'AZ'
            |when loc LIKE '%Arkansas%' then 'AR'
            |when loc LIKE '%California%' then 'CA'
            |when loc LIKE '%Colorado%' then 'CO'
            |when loc LIKE '%Connecticut%' then 'CT'
            |when loc LIKE '%Delaware%' then 'DE'
            |when loc LIKE '%Florida%' then 'FL'
            |when loc LIKE '%Georgia%' then 'GA'
            |when loc LIKE '%Hawaii%' then 'HI'
            |when loc LIKE '%Idaho%' then 'ID'
            |when loc LIKE '%Illinois%' then 'IL'
            |when loc LIKE '%Indiana%' then 'IN'
            |when loc LIKE '%Iowa%' then 'IA'
            |when loc LIKE '%Kansas%' then 'KS'
            |when loc LIKE '%Kentucky%' then 'KY'
            |when loc LIKE '%Louisiana%' then 'LA'
            |when loc LIKE '%Maine%' then 'ME'
            |when loc LIKE '%Maryland%' then 'MD'
            |when loc LIKE '%Massachusetts%' then 'MA'
            |when loc LIKE '%Michigan%' then 'MI'
            |when loc LIKE '%Minnesota%' then 'MN'
            |when loc LIKE '%Mississippi%' then 'MS'
            |when loc LIKE '%Missouri%' then 'MO'
            |when loc LIKE '%Montana%' then 'MT'
            |when loc LIKE '%Nebraska%' then 'NE'
            |when loc LIKE '%Nevada%' then 'NV'
            |when loc LIKE '%NewHampshire%' then 'NH'
            |when loc LIKE '%NewJersey%' then 'NJ'
            |when loc LIKE '%NewMexico%' then 'NM'
            |when loc LIKE '%NewYork%' then 'NY'
            |when loc LIKE '%NorthCarolina%' then 'NC'
            |when loc LIKE '%NorthDakota%' then 'ND'
            |when loc LIKE '%Ohio%' then 'OH'
            |when loc LIKE '%Oklahoma%' then 'OK'
            |when loc LIKE '%Oregon%' then 'OR'
            |when loc LIKE '%Pennsylvania%' then 'PA'
            |when loc LIKE '%RhodeIsland%' then 'RI'
            |when loc LIKE '%SouthCarolina%' then 'SC'
            |when loc LIKE '%SouthDakota%' then 'SD'
            |when loc LIKE '%Tennessee%' then 'TN'
            |when loc LIKE '%Texas%' then 'TX'
            |when loc LIKE '%Utah%' then 'UT'
            |when loc LIKE '%Vermont%' then 'VT'
            |when loc LIKE '%Virginia%' then 'VA'
            |when loc LIKE '%Washington%' then 'WA'
            |when loc LIKE '%WestVirginia%' then 'WV'
            |when loc LIKE '%Wisconsin%' then 'WI'
            |when loc LIKE '%Wyoming%' then 'WY'
            |when loc LIKE '% AL%' then 'AL'
            |when loc LIKE '% AK%' then 'AK'
            |when loc LIKE '% AZ%' then 'AZ'
            |when loc LIKE '% AR%' then 'AR'
            |when loc LIKE '% CA%' then 'CA'
            |when loc LIKE '% CO%' then 'CO'
            |when loc LIKE '% CT%' then 'CT'
            |when loc LIKE '% DE%' then 'DE'
            |when loc LIKE '% FL%' then 'FL'
            |when loc LIKE '% GA%' then 'GA'
            |when loc LIKE '% HI%' then 'HI'
            |when loc LIKE '% ID%' then 'ID'
            |when loc LIKE '% IL%' then 'IL'
            |when loc LIKE '% IN%' then 'IN'
            |when loc LIKE '% IA%' then 'IA'
            |when loc LIKE '% KS%' then 'KS'
            |when loc LIKE '% KY%' then 'KY'
            |when loc LIKE '% LA%' then 'LA'
            |when loc LIKE '% ME%' then 'ME'
            |when loc LIKE '% MD%' then 'MD'
            |when loc LIKE '% MA%' then 'MA'
            |when loc LIKE '% MI%' then 'MI'
            |when loc LIKE '% MN%' then 'MN'
            |when loc LIKE '% MS%' then 'MS'
            |when loc LIKE '% MO%' then 'MO'
            |when loc LIKE '% MT%' then 'MT'
            |when loc LIKE '% NE%' then 'NE'
            |when loc LIKE '% NV%' then 'NV'
            |when loc LIKE '% NH%' then 'NH'
            |when loc LIKE '% NJ%' then 'NJ'
            |when loc LIKE '% NM%' then 'NM'
            |when loc LIKE '% NY%' then 'NY'
            |when loc LIKE '% NC%' then 'NC'
            |when loc LIKE '% ND%' then 'ND'
            |when loc LIKE '% OH%' then 'OH'
            |when loc LIKE '% OK%' then 'OK'
            |when loc LIKE '% OR%' then 'OR'
            |when loc LIKE '% PA%' then 'PA'
            |when loc LIKE '% RI%' then 'RI'
            |when loc LIKE '% SC%' then 'SC'
            |when loc LIKE '% SD%' then 'SD'
            |when loc LIKE '% TN%' then 'TN'
            |when loc LIKE '% TX%' then 'TX'
            |when loc LIKE '% UT%' then 'UT'
            |when loc LIKE '% VT%' then 'VT'
            |when loc LIKE '% VA%' then 'VA'
            |when loc LIKE '% WA%' then 'WA'
            |when loc LIKE '% WV%' then 'WV'
            |when loc LIKE '% WI%' then 'WI'
            |when loc LIKE '% WY%' then 'WY'
            |                       else
            |                        loc
            |                        end as state from locate """.stripMargin)

        sql3RDD.createOrReplaceTempView("sql3RDD")


        val temp = sqlContext.sql(""" SELECT brandType, state, count(brandType) as type_count from sql3RDD group by state, brandType""")
        temp.createOrReplaceTempView("temp")
        val max = sqlContext.sql(""" SELECT state, max(type_count) as max_count from temp group by state""")
        max.createOrReplaceTempView("max")
        val brand_state = sqlContext.sql(""" SELECT brandType, t.state, type_count from temp t, max m where t.state = m.state and t.type_count = m.max_count """)
        brand_state.createOrReplaceTempView("brand_state")
        println ("**********************************")
        println("On Which hours More Tweets Were Done")
        println("**********************************")
        brand_state.show()

    }
  }
}
