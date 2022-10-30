package TwoPhaseCommitTest

/**
 * @author jiangfan
 * @date 2022/9/27 18:12
 */
import java.sql.{BatchUpdateException, DriverManager, PreparedStatement, SQLException, Timestamp}
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Date, Properties, UUID}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

class PostgreSink(props : Properties, config : ExecutionConfig) extends TwoPhaseCommitSinkFunction[(String,String,String,String),String,String](createTypeInformation[String].createSerializer(config),createTypeInformation[String].createSerializer(config)){
  private var transactionMap : Map[String,Array[(String,String,String,String)]] = Map()
  private var parsedQuery : PreparedStatement = _
  private val insertionString : String = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"

  override def invoke(transaction: String, value: (String,String,String,String), context: SinkFunction.Context[_]): Unit = {
    val LOG = LoggerFactory.getLogger(this.getClass)
    LOG.info("transaction========"+transaction)
    LOG.info("value======"+value)
    println(transaction+"invoke======"+value)
    val res = this.transactionMap.get(transaction)
    if(res.isDefined){
      var array = res.get
      array = array ++ Array(value)
      this.transactionMap += (transaction -> array)
    }else{
      val array = Array(value)
      this.transactionMap += (transaction -> array)
    }
    LOG.info("\n\nPassing through invoke\n\n")
    println("invoke---"+this.transactionMap)
    ()
  }

  override def beginTransaction(): String = {
    val LOG = LoggerFactory.getLogger(this.getClass)
    val identifier = UUID.randomUUID.toString
    LOG.info("\n\nPassing through beginTransaction\n\n")
    LOG.info("identifier===="+identifier)
    println("beginTransaction---"+this.transactionMap)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"beginTransaction identifier===="+identifier)
    identifier
  }

  override def preCommit(transaction: String): Unit = {
    val LOG = LoggerFactory.getLogger(this.getClass)
    LOG.info("start preCommit")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start preCommit")
    Thread.sleep(20000)
    try{
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"being addBatch")
      println("preCommit--------"+this.transactionMap)
      val tuple : Option[Array[(String,String,String,String)]]= this.transactionMap.get(transaction)
      if(tuple.isDefined){
        println("aaaaaaaaaaa====="+tuple.get.mkString)
        tuple.get.foreach( (value : (String,String,String,String)) => {
          LOG.info("\n\n"+value.toString()+"\n\n")
          println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"sql preCommit")
          println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+value.toString())
          this.parsedQuery.setString(1,value._2)
          this.parsedQuery.setString(2,value._3)
          this.parsedQuery.setString(3,value._4)
          this.parsedQuery.addBatch()
        })
      }
    }catch{
      case e:  Exception =>{e.printStackTrace()
                println("yihcang~~~~~~~~~~~~~")}
      case e : SQLException =>
        LOG.info("\n\nError when adding transaction to batch: SQLException\n\n")
      case f : ParseException =>
        LOG.info("\n\nError when adding transaction to batch: ParseException\n\n")
      case g : NoSuchElementException =>
        LOG.info("\n\nError when adding transaction to batch: NoSuchElementException\n\n")
      case h : Exception =>
        LOG.info("\n\nError when adding transaction to batch: Exception\n\n")
    }
    this.transactionMap = this.transactionMap.empty
    LOG.info("\n\nPassing through preCommit...\n\n")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end preCommit")
  }

  override def commit(transaction: String): Unit = {
    val LOG = LoggerFactory.getLogger(this.getClass)
    LOG.info("start commit~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start commit")
    Thread.sleep(20000)
    println("commit---"+this.transactionMap)
    if(this.parsedQuery != null) {
      LOG.info("\n\n" + this.parsedQuery.toString+ "\n\n")
    }
    try{
      this.parsedQuery.executeBatch
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"commit-executeBatch")
      val LOG = LoggerFactory.getLogger(this.getClass)
      LOG.info("\n\nExecuting batch\n\n")
    }catch{
      case e : SQLException =>
        val LOG = LoggerFactory.getLogger(this.getClass)
        LOG.info("\n\n"+"Error : SQLException"+"\n\n")
    }
//    this.transactionMap = this.transactionMap.empty
    LOG.info("\n\nPassing through commit...\n\n")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end commit")
  }

  override def abort(transaction: String): Unit = {
    val LOG = LoggerFactory.getLogger(this.getClass)
    this.transactionMap = this.transactionMap.empty
    LOG.info("\n\nPassing through abort...\n\n")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"Passing through abort...\n\n")
  }

  override def open(parameters: Configuration): Unit = {
    val LOG = LoggerFactory.getLogger(this.getClass)
    val driver = props.getProperty("driver")
    val url = props.getProperty("url")
    val user = props.getProperty("user")
    val password = props.getProperty("password")
    Class.forName(driver)
    val connection = DriverManager.getConnection(url + "?user=" + user + "&password=" + password)
    this.parsedQuery = connection.prepareStatement(insertionString)
    LOG.info("\n\nConfiguring BD conection parameters\n\n")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"open Configuring db")
  }
}