
#include "mysql.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "log_event.h"

#define EVENT_TYPE_OFFSET    4


typedef char		my_bool; /* Small bool */

#ifdef __cplusplus
extern "C" {
#endif
unsigned long cli_safe_read(MYSQL *mysql, my_bool *is_data_packet);
#ifdef __cplusplus
}
#endif  

#define CR_COMMANDS_OUT_OF_SYNC 2014
const char	*unknown_sqlstate= "HY000";

//table_map事件
Table_map_log_event *g_map = NULL;

typedef struct st_mysql_methods
{
  my_bool (*read_query_result)(MYSQL *mysql);
  my_bool (*advanced_command)(MYSQL *mysql,
			      enum enum_server_command command,
			      const unsigned char *header,
			      size_t header_length,
			      const unsigned char *arg,
			      size_t arg_length,
			      my_bool skip_check,
                              MYSQL_STMT *stmt);
  MYSQL_DATA *(*read_rows)(MYSQL *mysql,MYSQL_FIELD *mysql_fields,
			   unsigned int fields);
  MYSQL_RES * (*use_result)(MYSQL *mysql);
  void (*fetch_lengths)(unsigned long *to, 
			MYSQL_ROW column, unsigned int field_count);
  void (*flush_use_result)(MYSQL *mysql, my_bool flush_all_results);
  int (*read_change_user_result)(MYSQL *mysql);
#if !defined(MYSQL_SERVER) || defined(EMBEDDED_LIBRARY)
  MYSQL_FIELD * (*list_fields)(MYSQL *mysql);
  my_bool (*read_prepare_result)(MYSQL *mysql, MYSQL_STMT *stmt);
  int (*stmt_execute)(MYSQL_STMT *stmt);
  int (*read_binary_rows)(MYSQL_STMT *stmt);
  int (*unbuffered_fetch)(MYSQL *mysql, char **row);
  void (*free_embedded_thd)(MYSQL *mysql);
  const char *(*read_statistics)(MYSQL *mysql);
  my_bool (*next_result)(MYSQL *mysql);
  int (*read_rows_from_cursor)(MYSQL_STMT *stmt);
  void (*free_rows)(MYSQL_DATA *cur);
#endif
} MYSQL_METHODS;

#define simple_command(mysql, command, arg, length, skip_check) \
  ( (*(mysql)->methods->advanced_command)(mysql, command, 0, \
                                            0, arg, length, skip_check, NULL) \
  )

enum enum_slave_apply_event_and_update_pos_retval
{
  SLAVE_APPLY_EVENT_AND_UPDATE_POS_OK= 0,
  SLAVE_APPLY_EVENT_AND_UPDATE_POS_APPLY_ERROR= 1,
  SLAVE_APPLY_EVENT_AND_UPDATE_POS_UPDATE_POS_ERROR= 2,
  SLAVE_APPLY_EVENT_AND_UPDATE_POS_APPEND_JOB_ERROR= 3,
  SLAVE_APPLY_EVENT_AND_UPDATE_POS_MAX
};

//const int BINLOG_POS_INFO_SIZE= 8;
//const int BINLOG_DATA_SIZE_INFO_SIZE= 4;
const int BINLOG_POS_OLD_INFO_SIZE= 4;
const int BINLOG_FLAGS_INFO_SIZE= 2;
const int BINLOG_SERVER_ID_INFO_SIZE= 4;
//const int BINLOG_NAME_SIZE_INFO_SIZE= 4;

typedef unsigned long long int ulonglong; /* ulong or unsigned long long */
typedef long long int	longlong;
typedef longlong int64;
typedef ulonglong uint64;
typedef unsigned char	uchar;
typedef unsigned short uint16;
//typedef unsigned int uint32;
typedef unsigned long ulong;

int queue_event(const char* buf, uint event_len);

static inline void int2store(uchar *T, uint16 A)
{
  *((uint16*) T)= A;
}

/*
static inline void int3store(uchar *T, uint A)
{
  *(T)=   (uchar) (A);
  *(T+1)= (uchar) (A >> 8);
  *(T+2)= (uchar) (A >> 16);
}
*/

static inline void int4store(uchar *T, uint32 A)
{
  *((uint32*) T)= A;
}

/*
static inline void int8store(uchar *T, ulonglong A)
{
  *((ulonglong*) T)= A;
}
*/

int process_event(const char* buf, uint event_len)
{
    enum Log_event_type
    {

      UNKNOWN_EVENT= 0,
      START_EVENT_V3= 1,
      QUERY_EVENT= 2,
      STOP_EVENT= 3,
      ROTATE_EVENT= 4,
      INTVAR_EVENT= 5,
      LOAD_EVENT= 6,
      SLAVE_EVENT= 7,
      CREATE_FILE_EVENT= 8,
      APPEND_BLOCK_EVENT= 9,
      EXEC_LOAD_EVENT= 10,
      DELETE_FILE_EVENT= 11,

      NEW_LOAD_EVENT= 12,
      RAND_EVENT= 13,
      USER_VAR_EVENT= 14,
      FORMAT_DESCRIPTION_EVENT= 15,
      XID_EVENT= 16,
      BEGIN_LOAD_QUERY_EVENT= 17,
      EXECUTE_LOAD_QUERY_EVENT= 18,
    
      TABLE_MAP_EVENT = 19,

      PRE_GA_WRITE_ROWS_EVENT = 20,
      PRE_GA_UPDATE_ROWS_EVENT = 21,
      PRE_GA_DELETE_ROWS_EVENT = 22,
    

      WRITE_ROWS_EVENT_V1 = 23,
      UPDATE_ROWS_EVENT_V1 = 24,
      DELETE_ROWS_EVENT_V1 = 25,
    

      INCIDENT_EVENT= 26,
    

      HEARTBEAT_LOG_EVENT= 27,
    

      IGNORABLE_LOG_EVENT= 28,
      ROWS_QUERY_LOG_EVENT= 29,

      WRITE_ROWS_EVENT = 30,
      UPDATE_ROWS_EVENT = 31,
      DELETE_ROWS_EVENT = 32,
    
      GTID_LOG_EVENT= 33,
      ANONYMOUS_GTID_LOG_EVENT= 34,
    
      PREVIOUS_GTIDS_LOG_EVENT= 35,
    
      TRANSACTION_CONTEXT_EVENT= 36,
    
      VIEW_CHANGE_EVENT= 37,

      XA_PREPARE_LOG_EVENT= 38,

      ENUM_END_EVENT 
    };

    Log_event_type event_type= (Log_event_type)buf[EVENT_TYPE_OFFSET];
    
    Log_event* ev = NULL;
    const char *szErrorMsg;
    const Format_description_log_event format_ev = Format_description_log_event(4);

    ev= Log_event::read_log_event(buf, event_len, &szErrorMsg, &format_ev, 1);
    if (ev)
    {
         char szSQLString[10240];
         memset(szSQLString,0,sizeof(szSQLString));
         switch (ev->get_type_code())
         {
           case QUERY_EVENT:
               strcpy(szSQLString, ( static_cast< Query_log_event* >( ev ) ) -> query);	
               printf("szSQLString=%s\n",szSQLString);
               break;
           case TABLE_MAP_EVENT:
                //{数据库名、表名、字段个数}
                g_map = static_cast< Table_map_log_event* >( ev );
                break;
           case PRE_GA_WRITE_ROWS_EVENT:
           case WRITE_ROWS_EVENT:
           case WRITE_ROWS_EVENT_V1:
                printf("write event \n");
                if(g_map)
                    static_cast< Write_rows_log_event* >( ev )->print_verbose(g_map);
                break;
           case ROWS_QUERY_LOG_EVENT:
                strcpy(szSQLString, ( static_cast< Rows_query_log_event* >( ev ) ) -> m_rows_query);	
                printf("szSQLString=%s\n",szSQLString);
                break;          
            default:
                printf("default\n");
         }
    }
    
    return 0;
}

int main()
{
    MYSQL mysql;
    /**   1、连接MySQL服务器   *****/
    if( mysql_init(&mysql) == NULL )
    {
        printf("inital mysql handle error\n");
        return -1;
    }
    char host[32];
    char user[32];
    char passwd[32];
    char dbname[32];

    memset(host, sizeof(host), 0);
    memset(user, sizeof(user), 0);
    memset(passwd, sizeof(passwd), 0);
    memset(dbname, sizeof(dbname), 0);
    sprintf(host, "127.0.0.1");
    sprintf(user, "root");
    sprintf(passwd, "123456");
    //sprintf(dbname, "test");
    
    if (mysql_real_connect(&mysql,host,user,passwd,NULL,3306,NULL,0) == NULL)
    {
        printf("Failed to connect to database: Error: %s\n",mysql_error(&mysql));
        return -1;
    }
    else 
    {
        printf("Succeed to connect to database: \n");
    }

    /**   2、发送dump命令   *****/
    // allocate buffer
    /*
    int encoded_data_size = 8;
    int BINLOG_NAME_INFO_SIZE = strlen("binlog.000001");
    int allocation_size= 
      ::BINLOG_FLAGS_INFO_SIZE + ::BINLOG_SERVER_ID_INFO_SIZE +
      ::BINLOG_NAME_SIZE_INFO_SIZE + BINLOG_NAME_INFO_SIZE +
      ::BINLOG_POS_INFO_SIZE + ::BINLOG_DATA_SIZE_INFO_SIZE +
      encoded_data_size + 1;
    unsigned char* command_buffer= NULL;
    if (!(command_buffer= (unsigned char *) malloc(allocation_size)))
    {
        printf("Error:malloc \n");
        return -1;
    }
    unsigned char* ptr_buffer= command_buffer;

    int binlog_flags = 0;
    int server_id=99999;

    int2store(ptr_buffer, binlog_flags);
    ptr_buffer+= ::BINLOG_FLAGS_INFO_SIZE;
    int4store(ptr_buffer, server_id);
    ptr_buffer+= ::BINLOG_SERVER_ID_INFO_SIZE;
    int4store(ptr_buffer, static_cast<uint32>(BINLOG_NAME_INFO_SIZE));
    ptr_buffer+= ::BINLOG_NAME_SIZE_INFO_SIZE;
    memset(ptr_buffer, 0, BINLOG_NAME_INFO_SIZE);
    ptr_buffer+= BINLOG_NAME_INFO_SIZE;
    int8store(ptr_buffer, 4LL);
    ptr_buffer+= ::BINLOG_POS_INFO_SIZE;

    int4store(ptr_buffer, static_cast<uint32>(encoded_data_size));
    ptr_buffer+= ::BINLOG_DATA_SIZE_INFO_SIZE;
    int8store(ptr_buffer, 0);
    //gtid_executed.encode(ptr_buffer);
    ptr_buffer+= encoded_data_size;

    int command_size= ptr_buffer - command_buffer;
    */
    int BINLOG_NAME_INFO_SIZE = strlen("binlog.000034");
    int allocation_size= ::BINLOG_POS_OLD_INFO_SIZE +
      BINLOG_NAME_INFO_SIZE + ::BINLOG_FLAGS_INFO_SIZE +
      ::BINLOG_SERVER_ID_INFO_SIZE + 1;
    unsigned char* command_buffer= NULL;
    if (!(command_buffer= (unsigned char *) malloc(allocation_size)))
    { 
        printf("Error:malloc \n");
        return -1;
    }
    uchar* ptr_buffer= command_buffer;
  
    int4store(ptr_buffer, 4);
    ptr_buffer+= ::BINLOG_POS_OLD_INFO_SIZE;
    // See comment regarding binlog_flags above.
    int binlog_flags = 0;
    int2store(ptr_buffer, binlog_flags);
    ptr_buffer+= ::BINLOG_FLAGS_INFO_SIZE;
    int server_id=99999;
    int4store(ptr_buffer, server_id);
    ptr_buffer+= ::BINLOG_SERVER_ID_INFO_SIZE;
    memcpy(ptr_buffer, "binlog.000016", BINLOG_NAME_INFO_SIZE);
    ptr_buffer+= BINLOG_NAME_INFO_SIZE;

    int command_size= ptr_buffer - command_buffer;
    printf("command_size=%d\n", command_size);

    int iRet = simple_command(&mysql, COM_BINLOG_DUMP, command_buffer, command_size, 1);
    if (iRet)
    {
   
    }
    free(command_buffer);

    /**   3、读取event****/
    while(true)
    {

        ulong len= cli_safe_read(&mysql, NULL);

        /* Check if eof packet */
        if (len < 8 && mysql.net.read_pos[0] == 254)
        {
           printf("Slave : received end packet from server due to dump "
                                 "thread being killed on master. Dump threads are "
                                 "killed for example during master shutdown, "
                                 "explicitly by a user, or when the master receives "
                                 "a binlog send request from a duplicate server \n");
           return -1;
        }
        //const char *event_buf= (const char*)mysql.net.read_pos + 1;
        const char *event_buf= (const char*)mysql.net.buff + 1;
    
        //printf("event_buf=%s\n", event_buf);
        if(process_event(event_buf, len - 1))
        {
    
        }
      

        /***4、执行event*********/
        //Log_event *ev = NULL;
        //ev->apply_event(rli);
        usleep(5000);
    }
    return 0;
}


