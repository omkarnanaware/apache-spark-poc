package data.sparkscala.utils

object FrameworkConstants {

  final val DOT = "."
  final val COMMA = ","
  final val EMPTY_STRING = ""
  final val UNDERSCORE = "_"
  final val NONE = "none"
  final val BOOTSTRAP = "bootstrap"
  final val SERVERS = "servers"
  final val CONS_EXEC_MODE_BATCH = "batch"
  final val CONS_EXEC_MODE_STREAMING = "streaming"
  final val CONS_EXEC_MODE_STANDALONE = "standalone"
  final val CONS_EXEC_MODE_FLIE = "file"
  final val CONS_EXEC_MODE_RDBMS = "rdbms"
  final val CONS_EXEC_MODE_LOCAL = "local"
  final val CONS_KAFKA_PARSER_BASE_SCHEMA = "topic:string,partition:integer,offset:long,key:string,value:string"
  final val CONS_KAFKA_PARSER_GG_SCHEMA = "table:STRING,op_type:STRING,op_ts:STRING,current_ts:STRING,pos:DECIMAL,tokens:STRING,before:STRING,after:STRING"
  final val PROP_PARSER_SELECT = "topic,partition,offset,key,value,table,op_type,op_ts,current_ts,pos,tokens,before,after"
  final val PROP_PERSIST_SELECT = "topic,partition,offset,key,value,table,op_type,op_ts,current_ts,pos,tokens,before,data,type,result"
  final val PROP_PERSIST_SELECT_MIN = "topic,partition,offset,key,value,table,op_type,op_ts,current_ts,pos,tokens,before,after,data,type,result"
  final val PROP_DATA_PARSE_OPERATION_LIST = "I,U"
//  final val
//  final val
//  final val


}
