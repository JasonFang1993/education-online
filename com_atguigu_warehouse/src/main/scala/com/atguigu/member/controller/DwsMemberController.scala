package com.atguigu.member.controller

//import com.atguigu.member.bean.{DwsMember, DwsMember_Result}
import com.atguigu.member.bean.DwsMember
import com.atguigu.member.service.DwsMemberService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator

object DwsMemberController {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    System.setProperty("hadoop.home.dir","D:\\developer_tools\\hadoop-common-2.2.0-bin-master")
    val sparkConf = new SparkConf().setAppName("dws_member_import")
      .setMaster("local[*]")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")
      .set("spark.sql.shuffle.partitions","36")
    //          .set("spark.reducer.maxSizeInFilght", "96mb")
    //      .set("spark.shuffle.file.buffer", "64k")
    //      .set("spark.sql.shuffle.partitions", "45")
//          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//          .registerKryoClasses(Array(classOf[DwsMember]))
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    //    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
      //  DwsMemberService.importMember(sparkSession, "20190722") //根据用户信息聚合用户表数据
   DwsMemberService.importMemberUseApi(sparkSession, "20190722")
  }
}
