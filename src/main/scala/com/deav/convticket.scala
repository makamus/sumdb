package com.deav

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lxb on 2017/11/13.
  */
object Convticket {

  private var ssqlcontext:SQLContext =null ;
  private var sc:SparkContext =null;

  private def getSqlContext(): SQLContext ={
    val conf = new SparkConf().setAppName("Convticket").setMaster("yarn-client")
    sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext
  }

  def getSingleContext():SQLContext = {
    if(ssqlcontext==null){
      ssqlcontext = getSqlContext();
    }
    ssqlcontext
  }

  def main(args: Array[String]) {
    ssqlcontext = getSingleContext()
    System.setProperty("HADOOP_USER_NAME", "hive")
    //
    ssqlcontext.sql("drop table if exists tmp_internation_orders")
    ssqlcontext.sql("create table tmp_internation_orders as select user_id,d.order_id,d.passenger_uid, o.triptype, d.TRIPTYPE as triptype2, d.TRIPSEQUENCY, d.dep_city_name,d.arr_city_name,d.dep_code,d.arr_code,d.dep_time,d.create_time, o.insure_type, o.dt, d.arr_time, d.price from s_airline_order o join s_airline_order_detail d on o.order_id=d.order_id and o.orderstatue IN ('5','52','53','71','73','74','75') and (dep_country_type!='M' or arr_country_type!='M') ")
    val cnt = ssqlcontext.sql("select count(distinct order_id) as cnt from ( select order_id from tmp_internation_orders where triptype NOT IN ('RT', 'MP') group by order_id,passenger_uid having COUNT(1) > 1 ) t ").rdd.collect().array.head.getAs[Long](0)
    val ordernum = ssqlcontext.sql("select count(distinct order_id) as cnt from  tmp_internation_orders ").rdd.collect().array.head.getAs[Long](0)
    val st="insert into interfly_sum_super values (\"convticket\",\"" + "{'cnt':"+cnt+",'ordernum':"+ordernum + "}\" )"
    DBUtils.etl_update("delete from interfly_sum_super where keyv='convticket'")
    DBUtils.etl_insert(st);

    //中转城市数据
    var df= ssqlcontext.sql("select * from ( select dep_city_name,count(distinct s.order_id) as city_num from tmp_internation_orders s join ( select order_id from tmp_internation_orders where triptype NOT IN ('RT', 'MP') group by order_id,passenger_uid having COUNT(1) > 1 ) t on s.order_id=t.order_id  and s.TRIPSEQUENCY>1 group by dep_city_name ) tt order by city_num desc limit 15")
    val convCity = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val sql = "insert into interfly_sum_super values (\"topcityconv\",\""+ convCity + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='topcityconv'")
    DBUtils.etl_insert(sql);

    //订单提前预定时间
    df = ssqlcontext.sql("select sum(case when diffday<3 then 1 else 0 end ) as 3days, sum(case when diffday<7 and diffday>=3 then 1 else 0 end ) as 7days, sum(case when diffday<15 and diffday>=7 then 1 else 0 end ) as 15days, sum(case when diffday<30 and diffday>=15 then 1 else 0 end ) as 30days, sum(case when diffday<60 and diffday>=30 then 1 else 0 end ) as 60days, sum(case when diffday>=60 then 1 else 0 end ) as above60days from (select distinct order_id, datediff(dep_time, create_time) as diffday from  tmp_internation_orders ) t ")
    val preday = df.toJSON.collect.mkString.replace("\"","'")
    val predaysql = "insert into interfly_sum_super values (\"predaytb\",\""+ preday + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='predaytb'")
    DBUtils.etl_insert(predaysql);

    //往返程/单程分布
    df = ssqlcontext.sql("select triptype, count(distinct order_id) as cnt from tmp_internation_orders where triptype in ('OW','RT') group by triptype")
    val rtstr = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val rtsql = "insert into interfly_sum_super values (\"obrt\",\""+ rtstr + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='obrt'")
    DBUtils.etl_insert(rtsql);

    //保险购买情况
    df = ssqlcontext.sql("select year,count(distinct order_id) as order_num, count( case when insure_type!='NULL' then order_id end) as insure_num from (select distinct order_id, insure_type, substr(dt,0,4) year from tmp_internation_orders ) t group by year ")
    val insureStr = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val insureSql = "insert into interfly_sum_super values (\"insure_sum\",\""+ insureStr + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='insure_sum'")
    DBUtils.etl_insert(insureSql);

    ssqlcontext.sql("drop table if exists tmp.order_cost_time")
    //行程耗时分布
    ssqlcontext.sql("create table tmp.order_cost_time as select s.order_id,sum( (unix_timestamp(arr_time)-unix_timestamp(dep_time) )/3600 ) as ordertime from tmp_internation_orders s left join (select order_id  from tmp_internation_orders where triptype NOT IN ('RT', 'MP') group by order_id,passenger_uid having COUNT(1) > 1 ) t on s.order_id=t.order_id where t.order_id is null group by s.order_id union all select order_id,(unix_timestamp(arr_time)-unix_timestamp(dep_time) )/3600 as ordertime from ( select s.order_id,min(dep_time) as dep_time, max(arr_time) as arr_time  from tmp_internation_orders s join (select order_id  from tmp_internation_orders where triptype NOT IN ('RT', 'MP') group by order_id,passenger_uid having COUNT(1) > 1 ) t on s.order_id=t.order_id group by s.order_id ) tt ")
    df = ssqlcontext.sql("select sum(case when ordertime<3 then 1 else 0 end ) as 3hours, sum(case when ordertime<5 and ordertime>=3 then 1 else 0 end ) as 5hours, sum(case when ordertime<10 and ordertime>=5 then 1 else 0 end ) as 10hours, sum(case when ordertime<15 and ordertime>=10 then 1 else 0 end ) as 15hours, sum(case when ordertime<24 and ordertime>=15 then 1 else 0 end ) as 24hours, sum(case when ordertime>=24 then 1 else 0 end ) as above24hours from tmp.order_cost_time")
    val costhour = df.toJSON.collect.mkString.replace("\"","'")
    val costhourSQL = "insert into interfly_sum_super values (\"costhour\",\""+ costhour + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='costhour'")
    DBUtils.etl_insert(costhourSQL);

    //搜索购买比例分布情况
    df = ssqlcontext.sql("select sum(case when t.user_id is null then 1 else 0 end ) as nobuyusers,count(t.user_id) as buyusers from (select distinct phoneid from sp_class.s_search_hb_detail where interroute='false') s left join (select distinct user_id from tmp_internation_orders WHERE dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) ) t on s.phoneid=t.user_id ")
    val searchbuy = df.toJSON.collect.mkString.replace("\"","'")
    val searchbuySQL = "insert into interfly_sum_super values (\"searchbuy_sum\",\""+ searchbuy + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='searchbuy_sum'")
    DBUtils.etl_insert(searchbuySQL);

    //未购买用户搜索次数分布情况
    df = ssqlcontext.sql("select sum(case when s.searchnum=1 then 1 else 0 end) as 1times,sum(case when s.searchnum>=2 and s.searchnum<=5 then 1 else 0 end) as 5times, sum(case when s.searchnum>=6 and s.searchnum<=10 then 1 else 0 end) as 10times, sum(case when s.searchnum>10 then 1 else 0 end) as above10times  from (select phoneid,count(1) as searchnum from sp_class.s_search_hb_detail where interroute='false' group by phoneid) s left join (select distinct user_id from tmp_internation_orders WHERE dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) )t on s.phoneid=t.user_id where t.user_id is null ")
    val searchtimes = df.toJSON.collect.mkString.replace("\"","'")
    val searchtimesSQL = "insert into interfly_sum_super values (\"searchtimes\",\""+ searchtimes + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='searchtimes'")
    DBUtils.etl_insert(searchtimesSQL);

    //搜索目的地城市分布
    df = ssqlcontext.sql("select NVL(a.cityname,t.arrcity) as cityname,sum(peoplesnum) as peoplesnum, sum(timesnum) as timesnum from (select arrcity,count(distinct uid) as peoplesnum,count(1) as timesnum from sp_class.s_search_hb_detail where interroute='false' group by arrcity  ) t left join  airport_info a on  a.airportcode=t.arrcity  group by NVL(a.cityname,t.arrcity) order by sum(peoplesnum) desc limit 15")
    val search_city = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val search_citySql = "insert into interfly_sum_super values (\"search_arrcity\",\""+ search_city + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='search_arrcity'")
    DBUtils.etl_insert(search_citySql);

    //搜索出发城市的分布情况
    df = ssqlcontext.sql("select NVL(a.cityname,t.depcity) as cityname,sum(peoplesnum) as peoplesnum, sum(timesnum) as timesnum from (select depcity,count(distinct uid) as peoplesnum,count(1) as timesnum from sp_class.s_search_hb_detail where interroute='false' group by depcity  ) t left join  airport_info a on  a.airportcode=t.depcity  group by  NVL(a.cityname,t.depcity)  order by sum(peoplesnum) desc limit 15 ")
    val search_depcity = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val search_depcitySql = "insert into interfly_sum_super values (\"search_depcity\",\""+ search_depcity + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='search_depcity'")
    DBUtils.etl_insert(search_depcitySql);

    //国际机票用户在购买机票前搜索情况
    ssqlcontext.sql("drop table if exists tmp.order_dep_arr_code")
    var convorder= ssqlcontext.sql("CREATE TABLE tmp.order_dep_arr_code as  select s.order_id, s.user_id, s.dep_code,s.arr_code,s.create_time FROM ( select * from tmp_internation_orders where dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) ) s left join ( select order_id from tmp_internation_orders where dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) and triptype NOT IN ('RT', 'MP') group by order_id,passenger_uid having COUNT(1) > 1 ) t on s.order_id=t.order_id where  t.order_id is null  UNION ALL select s.order_id,max(user_id) as user_id,max(case when TRIPSEQUENCY=1 then dep_code end) as dep_code,max(case when TRIPSEQUENCY=t.max_seq then arr_code end) as arr_code,max(create_time) as create_time  from tmp_internation_orders s join ( select order_id,max(TRIPSEQUENCY) as max_seq from tmp_internation_orders where dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) and triptype NOT IN ('RT', 'MP') group by order_id,passenger_uid having COUNT(1) > 1 ) t on s.order_id=t.order_id and s.dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) group by s.order_id")
    df = ssqlcontext.sql("select sitem,count(1) as stimes from ( select CASE WHEN searchnum<5 THEN 'Below5' WHEN searchnum<10 AND searchnum>=5 THEN '5-10' WHEN searchnum<20 AND searchnum>=10 THEN '10-20' WHEN searchnum<50 AND searchnum>=20 THEN '20-50' ELSE 'Above50' end as sitem,order_id from (select o.order_id,count(1) as searchnum FROM tmp.order_dep_arr_code o join (select phoneid,depcity,arrcity,dt from sp_class.s_search_hb_detail where interroute='false') s  on (o.user_id=s.phoneid and o.dep_code=s.depcity and o.arr_code=s.arrcity and substr(o.create_time,0,10)>=s.dt ) group by o.order_id )t  ) tt group by sitem")
    //ssqlcontext.sql(" select s.order_id, s.user_id, s.dep_code,s.arr_code,s.create_time FROM ( select * from tmp_internation_orders where dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) ) s left join ( select order_id from tmp_internation_orders where dt>=add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),-6) and triptype NOT IN ('RT', 'MP') group by order_id,passenger_uid having COUNT(1) > 1 ) t on s.order_id=t.order_id where  t.order_id is null ")
    val search_item = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val search_itemSql = "insert into interfly_sum_super values (\"order_search_sum\",\""+ search_item + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='order_search_sum'")
    DBUtils.etl_insert(search_itemSql);

    //犹豫时间，按照截止当前为止最近的订单，取首次搜索和产生该订单的时间差
    df = ssqlcontext.sql("select diffdays,count(1) as num from ( SELECT CASE WHEN sptime<=3 THEN '3DAYS' when sptime<=7 and sptime>3 THEN '7DAYS' when sptime<=15 and sptime>7 THEN '15DAYS' when sptime<=30 and sptime>15 THEN '30DAYS' when sptime<=60 and sptime>30 THEN '60DAYS'  when sptime>60 THEN 'above60DAYS' end  as diffdays, order_id from ( SELECT datediff(ordertime,old_searchtime) as sptime,order_id FROM ( select o.order_id,min(s.dt) as old_searchtime, max(substr(o.create_time,0,10)) as ordertime from tmp.order_dep_arr_code o join (select phoneid,depcity,arrcity,dt from sp_class.s_search_hb_detail where interroute='false') s  on (o.user_id=s.phoneid and o.dep_code=s.depcity and o.arr_code=s.arrcity and substr(o.create_time,0,10)>=s.dt ) group by o.order_id ) tt  ) ttt )op group by diffdays")
    val diffdays = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val diffdaysSql = "insert into interfly_sum_super values (\"order_search_diffday\",\""+ diffdays + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='order_search_diffday'")
    DBUtils.etl_insert(diffdaysSql);

    //机票单价情况
    df = ssqlcontext.sql("select pricearea, count(1) as num FROM (select case when orderprice<1000 then 'low1K' when orderprice>=1000 and orderprice<2000 then '1K-2k' when orderprice>=2000 and orderprice<3000 then '2K-3k' when orderprice>=3000 and orderprice<4000 then '3K-4k' when orderprice>=4000  then 'Above4k' end as pricearea, order_id,dep_code  from ( select order_id,dep_code,sum(price) as orderprice from tmp_internation_orders group by order_id,dep_code ) t )tt group by pricearea")
    val priceorder = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val priceorderSql = "insert into interfly_sum_super values (\"pricearea\",\""+ priceorder + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='pricearea'")
    DBUtils.etl_insert(priceorderSql);

    //目的地分布占比，出发地分布占比
    df = ssqlcontext.sql("select cityname,count(1) as num  from ( select a.cityname,o.order_id from tmp.order_dep_arr_code o left join airport_info a on o.dep_code=a.airportcode ) t group by cityname order by count(1) desc limit 15")
    val depcity = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val depcitySql = "insert into interfly_sum_super values (\"depcitytop\",\""+ depcity + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='depcitytop'")
    DBUtils.etl_insert(depcitySql);

    df = ssqlcontext.sql("select cityname,count(1) as num  from ( select a.cityname,o.order_id from tmp.order_dep_arr_code o left join airport_info a on o.arr_code=a.airportcode ) t group by cityname order by count(1) desc limit 15")
    val arrcity = df.toJSON.collect().mkString("[",",","]").replace("\"","'")
    val arrcitySql = "insert into interfly_sum_super values (\"arrcitytop\",\""+ arrcity + "\") " ;
    DBUtils.etl_update("delete from interfly_sum_super where keyv='arrcitytop'")
    DBUtils.etl_insert(arrcitySql);


    ssqlcontext.sql("drop table if exists tmp.order_dep_arr_code")
    ssqlcontext.sql("drop table if exists tmp_internation_orders")
    sc.stop()
  }

}
