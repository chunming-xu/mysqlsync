/*
   Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "log_event.h"
//#include <base64.h>
//#include <my_bitmap.h>
#include <map>

using std::min;
using std::max;
//char _dig_vec_upper[64];
char _dig_vec_upper[] =
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

#define my_b_write(info,Buffer,Count) \
 ((info)->write_pos + (Count) <=(info)->write_end ?\
  (memcpy((info)->write_pos, (Buffer), (size_t)(Count)),\
   ((info)->write_pos+=(Count)),0) : \
   (*(info)->write_function)((info),(uchar *)(Buffer),(Count)))

void int2store(uchar *T, uint16 A)
{
  *((uint16*) T)= A;
}

void int4store(uchar *T, uint32 A)
{
  *((uint32*) T)= A;
}
char *octet2hex(char *to, const char *str, uint len)
{
  const char *str_end= str + len; 
  for (; str != str_end; ++str)
  {
    *to++= _dig_vec_upper[((uchar) *str) >> 4];
    *to++= _dig_vec_upper[((uchar) *str) & 0x0F];
  }
  *to= '\0';
  return to;
}

/**
  BINLOG_CHECKSUM variable.
*/
const char *binlog_checksum_type_names[]= {
  "NONE",
  "CRC32",
  0
};

unsigned int binlog_checksum_type_length[]= {
  sizeof("NONE") - 1,
  sizeof("CRC32") - 1,
  0
};
/*
TYPELIB binlog_checksum_typelib=
{
  array_elements(binlog_checksum_type_names) - 1, "",
  binlog_checksum_type_names,
  binlog_checksum_type_length
};*/


#define log_cs	&my_charset_latin1

/*
  Size of buffer for printing a double in format %.<PREC>g

  optional '-' + optional zero + '.'  + PREC digits + 'e' + sign +
  exponent digits + '\0'
*/
#define FMT_G_BUFSIZE(PREC) (3 + (PREC) + 5 + 1)

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
static int rows_event_stmt_cleanup(Relay_log_info const *rli, THD* thd);

static const char *HA_ERR(int i)
{
  /* 
    This function should only be called in case of an error
    was detected 
   */
  DBUG_ASSERT(i != 0);
  switch (i) {
  case HA_ERR_KEY_NOT_FOUND: return "HA_ERR_KEY_NOT_FOUND";
  case HA_ERR_FOUND_DUPP_KEY: return "HA_ERR_FOUND_DUPP_KEY";
  case HA_ERR_RECORD_CHANGED: return "HA_ERR_RECORD_CHANGED";
  case HA_ERR_WRONG_INDEX: return "HA_ERR_WRONG_INDEX";
  case HA_ERR_CRASHED: return "HA_ERR_CRASHED";
  case HA_ERR_WRONG_IN_RECORD: return "HA_ERR_WRONG_IN_RECORD";
  case HA_ERR_OUT_OF_MEM: return "HA_ERR_OUT_OF_MEM";
  case HA_ERR_NOT_A_TABLE: return "HA_ERR_NOT_A_TABLE";
  case HA_ERR_WRONG_COMMAND: return "HA_ERR_WRONG_COMMAND";
  case HA_ERR_OLD_FILE: return "HA_ERR_OLD_FILE";
  case HA_ERR_NO_ACTIVE_RECORD: return "HA_ERR_NO_ACTIVE_RECORD";
  case HA_ERR_RECORD_DELETED: return "HA_ERR_RECORD_DELETED";
  case HA_ERR_RECORD_FILE_FULL: return "HA_ERR_RECORD_FILE_FULL";
  case HA_ERR_INDEX_FILE_FULL: return "HA_ERR_INDEX_FILE_FULL";
  case HA_ERR_END_OF_FILE: return "HA_ERR_END_OF_FILE";
  case HA_ERR_UNSUPPORTED: return "HA_ERR_UNSUPPORTED";
  case HA_ERR_TOO_BIG_ROW: return "HA_ERR_TOO_BIG_ROW";
  case HA_WRONG_CREATE_OPTION: return "HA_WRONG_CREATE_OPTION";
  case HA_ERR_FOUND_DUPP_UNIQUE: return "HA_ERR_FOUND_DUPP_UNIQUE";
  case HA_ERR_UNKNOWN_CHARSET: return "HA_ERR_UNKNOWN_CHARSET";
  case HA_ERR_WRONG_MRG_TABLE_DEF: return "HA_ERR_WRONG_MRG_TABLE_DEF";
  case HA_ERR_CRASHED_ON_REPAIR: return "HA_ERR_CRASHED_ON_REPAIR";
  case HA_ERR_CRASHED_ON_USAGE: return "HA_ERR_CRASHED_ON_USAGE";
  case HA_ERR_LOCK_WAIT_TIMEOUT: return "HA_ERR_LOCK_WAIT_TIMEOUT";
  case HA_ERR_LOCK_TABLE_FULL: return "HA_ERR_LOCK_TABLE_FULL";
  case HA_ERR_READ_ONLY_TRANSACTION: return "HA_ERR_READ_ONLY_TRANSACTION";
  case HA_ERR_LOCK_DEADLOCK: return "HA_ERR_LOCK_DEADLOCK";
  case HA_ERR_CANNOT_ADD_FOREIGN: return "HA_ERR_CANNOT_ADD_FOREIGN";
  case HA_ERR_NO_REFERENCED_ROW: return "HA_ERR_NO_REFERENCED_ROW";
  case HA_ERR_ROW_IS_REFERENCED: return "HA_ERR_ROW_IS_REFERENCED";
  case HA_ERR_NO_SAVEPOINT: return "HA_ERR_NO_SAVEPOINT";
  case HA_ERR_NON_UNIQUE_BLOCK_SIZE: return "HA_ERR_NON_UNIQUE_BLOCK_SIZE";
  case HA_ERR_NO_SUCH_TABLE: return "HA_ERR_NO_SUCH_TABLE";
  case HA_ERR_TABLE_EXIST: return "HA_ERR_TABLE_EXIST";
  case HA_ERR_NO_CONNECTION: return "HA_ERR_NO_CONNECTION";
  case HA_ERR_NULL_IN_SPATIAL: return "HA_ERR_NULL_IN_SPATIAL";
  case HA_ERR_TABLE_DEF_CHANGED: return "HA_ERR_TABLE_DEF_CHANGED";
  case HA_ERR_NO_PARTITION_FOUND: return "HA_ERR_NO_PARTITION_FOUND";
  case HA_ERR_RBR_LOGGING_FAILED: return "HA_ERR_RBR_LOGGING_FAILED";
  case HA_ERR_DROP_INDEX_FK: return "HA_ERR_DROP_INDEX_FK";
  case HA_ERR_FOREIGN_DUPLICATE_KEY: return "HA_ERR_FOREIGN_DUPLICATE_KEY";
  case HA_ERR_TABLE_NEEDS_UPGRADE: return "HA_ERR_TABLE_NEEDS_UPGRADE";
  case HA_ERR_TABLE_READONLY: return "HA_ERR_TABLE_READONLY";
  case HA_ERR_AUTOINC_READ_FAILED: return "HA_ERR_AUTOINC_READ_FAILED";
  case HA_ERR_AUTOINC_ERANGE: return "HA_ERR_AUTOINC_ERANGE";
  case HA_ERR_GENERIC: return "HA_ERR_GENERIC";
  case HA_ERR_RECORD_IS_THE_SAME: return "HA_ERR_RECORD_IS_THE_SAME";
  case HA_ERR_LOGGING_IMPOSSIBLE: return "HA_ERR_LOGGING_IMPOSSIBLE";
  case HA_ERR_CORRUPT_EVENT: return "HA_ERR_CORRUPT_EVENT";
  case HA_ERR_ROWS_EVENT_APPLY : return "HA_ERR_ROWS_EVENT_APPLY";
  case HA_ERR_FK_DEPTH_EXCEEDED : return "HA_ERR_FK_DEPTH_EXCEEDED";
  case HA_ERR_INNODB_READ_ONLY: return "HA_ERR_INNODB_READ_ONLY";
  case HA_ERR_COMPUTE_FAILED: return "HA_ERR_COMPUTE_FAILED";
  }
  return "No Error!";
}

/**
   Error reporting facility for Rows_log_event::do_apply_event

   @param level     error, warning or info
   @param ha_error  HA_ERR_ code
   @param rli       pointer to the active Relay_log_info instance
   @param thd       pointer to the slave thread's thd
   @param table     pointer to the event's table object
   @param type      the type of the event
   @param log_name  the master binlog file name
   @param pos       the master binlog file pos (the next after the event)

*/
static void inline slave_rows_error_report(enum loglevel level, int ha_error,
                                           Relay_log_info const *rli, THD *thd,
                                           TABLE *table, const char * type,
                                           const char *log_name, ulong pos)
{
  const char *handler_error= (ha_error ? HA_ERR(ha_error) : NULL);
  bool is_group_replication_applier_channel=
    channel_map.is_group_replication_channel_name((const_cast<Relay_log_info *>(rli))->get_channel(), true);
  char buff[MAX_SLAVE_ERRMSG], *slider;
  const char *buff_end= buff + sizeof(buff);
  size_t len;
  Diagnostics_area::Sql_condition_iterator it=
    thd->get_stmt_da()->sql_conditions();
  const Sql_condition *err;
  buff[0]= 0;

  for (err= it++, slider= buff; err && slider < buff_end - 1;
       slider += len, err= it++)
  {
    len= my_snprintf(slider, buff_end - slider,
                     " %s, Error_code: %d;", err->message_text(),
                     err->mysql_errno());
  }
  if (is_group_replication_applier_channel)
  {
    if (ha_error != 0)
    {
      rli->report(level, thd->is_error() ? thd->get_stmt_da()->mysql_errno() :
                  ER_UNKNOWN_ERROR, "Could not execute %s event on table %s.%s;"
                  "%s handler error %s",
                  type, table->s->db.str, table->s->table_name.str,
                  buff, handler_error == NULL ? "<unknown>" : handler_error);
    }
    else
    {
      rli->report(level, thd->is_error() ? thd->get_stmt_da()->mysql_errno() :
                  ER_UNKNOWN_ERROR, "Could not execute %s event on table %s.%s;"
                  "%s", type, table->s->db.str, table->s->table_name.str,
                  buff);
    }
  }
  else
  {
    if (ha_error != 0)
    {
      rli->report(level, thd->is_error() ? thd->get_stmt_da()->mysql_errno() :
                  ER_UNKNOWN_ERROR, "Could not execute %s event on table %s.%s;"
                  "%s handler error %s; "
                  "the event's master log %s, end_log_pos %lu",
                  type, table->s->db.str, table->s->table_name.str,
                  buff, handler_error == NULL ? "<unknown>" : handler_error,
                  log_name, pos);
    }
    else
    {
      rli->report(level, thd->is_error() ? thd->get_stmt_da()->mysql_errno() :
                  ER_UNKNOWN_ERROR, "Could not execute %s event on table %s.%s;"
                  "%s the event's master log %s, end_log_pos %lu",
                  type, table->s->db.str, table->s->table_name.str,
                  buff, log_name, pos);
    }
  }
}

static void set_thd_db(THD *thd, const char *db, size_t db_len)
{
  char lcase_db_buf[NAME_LEN +1]; 
  LEX_CSTRING new_db;
  new_db.length= db_len;
  if (lower_case_table_names)
  {
    my_stpcpy(lcase_db_buf, db); 
    my_casedn_str(system_charset_info, lcase_db_buf);
    new_db.str= lcase_db_buf;
  }
  else 
    new_db.str= (char*) db;

  new_db.str= (char*) rpl_filter->get_rewrite_db(new_db.str,
                                                 &new_db.length);
  thd->set_db(new_db);
}

#endif


static inline void int3store(uchar *T, uint A)
{
  *(T)=   (uchar) (A);
  *(T+1)= (uchar) (A >> 8);
  *(T+2)= (uchar) (A >> 16);
}

static inline void int8store(uchar *T, ulonglong A)
{
  *((ulonglong*) T)= A;
}

uchar *net_store_length(uchar *packet, ulonglong length)
{
  if (length < (ulonglong) 251LL)
  {
    *packet=(uchar) length;
    return packet+1;
  }
  /* 251 is reserved for NULL */
  if (length < (ulonglong) 65536LL)
  {
    *packet++=252;
    int2store(packet,(uint) length);
    return packet+2;
  }
  if (length < (ulonglong) 16777216LL)
  {
    *packet++=253;
    int3store(packet,(ulong) length);
    return packet+3;
  }
  *packet++=254;
  int8store(packet,length);
  return packet+8;
}
#define bitmap_buffer_size(bits) (((bits)+31)/32)*4
#define MY_ALIGN(A,L)	(((A) + (L) - 1) & ~((L) - 1))
#define ALIGN_SIZE(A)	MY_ALIGN((A),sizeof(double))

void create_last_word_mask(MY_BITMAP *map)
{
  /* Get the number of used bits (1..8) in the last byte */
  unsigned int const used= 1U + ((map->n_bits-1U) & 0x7U);

  /*
    Create a mask with the upper 'unused' bits set and the lower 'used'
    bits clear. The bits within each byte is stored in big-endian order.
   */
  unsigned char const mask= (~((1 << used) - 1)) & 255;

  /*
    The first bytes are to be set to zero since they represent real  bits
    in the bitvector. The last bytes are set to 0xFF since they  represent
    bytes not used by the bitvector. Finally the last byte contains  bits
    as set by the mask above.
  */
  unsigned char *ptr= (unsigned char*)&map->last_word_mask;

  /* Avoid out-of-bounds read/write if we have zero bits. */
  map->last_word_ptr= map->n_bits == 0 ? map->bitmap :
    map->bitmap + no_words_in_map(map) - 1;

  switch (no_bytes_in_map(map) & 3) {
  case 1:
    map->last_word_mask= ~0U;
    ptr[0]= mask;
    return;
  case 2:
    map->last_word_mask= ~0U;
    ptr[0]= 0;
    ptr[1]= mask;
    return;
  case 3:
    map->last_word_mask= 0U;
    ptr[2]= mask;
    ptr[3]= 0xFFU;
    return;
  case 0:
    map->last_word_mask= 0U;
    ptr[3]= mask;
    return;
  }
}
static inline void bitmap_clear_all(MY_BITMAP *map)
{
  memset(map->bitmap, 0, 4 * no_words_in_map(map));
}
my_bool bitmap_init(MY_BITMAP *map, my_bitmap_map *buf, uint n_bits,
		    my_bool thread_safe )
{
  if (!buf)
  {
    uint size_in_bytes= bitmap_buffer_size(n_bits);
    uint extra= 0;

    if (thread_safe)
    {
      size_in_bytes= ALIGN_SIZE(size_in_bytes);
      extra= sizeof(pthread_mutex_t);
    }
    map->mutex= 0;

    if (!(buf= (my_bitmap_map*) malloc(size_in_bytes+extra)))
      return 1;

    if (thread_safe)
    {
      map->mutex= (pthread_mutex_t *) ((char*) buf + size_in_bytes);
      pthread_mutex_init( map->mutex, NULL);
    }

  }

  else
  {
    map->mutex= NULL;
  }

  map->bitmap= buf;
  map->n_bits= n_bits;
  create_last_word_mask(map);
  bitmap_clear_all(map);
  return 0;
}

Rows_log_event::Rows_log_event(const char *buf, uint event_len,
                               const Format_description_event
                               *description_event)
: binary_log::Rows_event(buf, event_len, description_event),
  Log_event(header(), footer()),
  m_row_count(0),
#ifndef MYSQL_CLIENT
  //m_table(NULL),
#endif
  m_rows_buf(0), m_rows_cur(0), m_rows_end(0)
#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
    , m_curr_row(NULL), m_curr_row_end(NULL), m_key(NULL), m_key_info(NULL),
    m_distinct_keys(Key_compare(&m_key_info)), m_distinct_key_spare_buf(NULL)
#endif
{
  /*
  if (m_extra_row_data)
    DBUG_EXECUTE_IF("extra_row_data_check",
                    check_extra_data(m_extra_row_data););
  */

  /*
     m_cols and m_cols_ai are of the type MY_BITMAP, which are members of
     class Rows_log_event, and are used while applying the row events on
     the slave.
     The bitmap integer is initialized by copying the contents of the
     vector column_before_image for m_cols.bitamp, and vector
     column_after_image for m_cols_ai.bitmap. m_cols_ai is only initialized
     for UPDATE_ROWS_EVENTS, else it is equal to the before image.
  */
  memset(&m_cols, 0, sizeof(m_cols));
  /* if bitmap_init fails, is_valid will be set to false */
  if (!bitmap_init(&m_cols,
                          m_width <= sizeof(m_bitbuf) * 8 ? m_bitbuf : NULL,
                          m_width,
                          false))
  {
    if (!columns_before_image.empty())
    {
      memcpy(m_cols.bitmap, &columns_before_image[0], (m_width + 7) / 8);
      create_last_word_mask(&m_cols);
    
    } //end if columns_before_image.empty()
    else
    m_cols.bitmap= NULL;
  }
  else
  {
    // Needed because bitmap_init() does not set it to null on failure
    m_cols.bitmap= NULL;
    return;
  }
  m_cols_ai.bitmap= m_cols.bitmap; //See explanation below while setting is_valid.

  if ((m_type == binary_log::UPDATE_ROWS_EVENT) ||
      (m_type == binary_log::UPDATE_ROWS_EVENT_V1))
  {
    /* if bitmap_init fails, is_valid will be set to false*/
    if (!bitmap_init(&m_cols_ai,
                            m_width <= sizeof(m_bitbuf_ai) * 8 ?
                                        m_bitbuf_ai : NULL,
                            m_width,
                            false))
    {
      if (!columns_after_image.empty())
      {
        memcpy(m_cols_ai.bitmap, &columns_after_image[0], (m_width + 7) / 8);
        create_last_word_mask(&m_cols_ai);
      }
      else
        m_cols_ai.bitmap= NULL;
    }
    else
    {
      // Needed because bitmap_init() does not set it to null on failure
      m_cols_ai.bitmap= 0;
      return;
    }
  }


  /*
    m_rows_buf, m_cur_row and m_rows_end are pointers to the vector rows.
    m_rows_buf is the pointer to the first byte of first row in the event.
    m_curr_row points to current row being applied on the slave. Initially,
    this points to the same element as m_rows_buf in the vector.
    m_rows_end points to the last byte in the last row in the event.

    These pointers are used while applying the events on to the slave, and
    are not required for decoding.
  */
  if (!row.empty())
  {
    m_rows_buf= &row[0];
#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
    m_curr_row= m_rows_buf;
#endif
    m_rows_end= m_rows_buf + row.size() - 1;
    m_rows_cur= m_rows_end;
  }
  /*
    -Check that malloc() succeeded in allocating memory for the row
     buffer and the COLS vector.
    -Checking that an Update_rows_log_event
     is valid is done while setting the Update_rows_log_event::is_valid
  */
  if (m_rows_buf && m_cols.bitmap)
    is_valid_param= true;
}
void bitmap_free(MY_BITMAP *map)
{
  if (map->bitmap)
  {
    if (map->mutex)
      pthread_mutex_destroy(map->mutex);

    my_free(map->bitmap);
    map->bitmap=0;
  }
}

Rows_log_event::~Rows_log_event()
{
  if (m_cols.bitmap)
  {
    if (m_cols.bitmap == m_bitbuf) // no my_malloc happened
      m_cols.bitmap= 0; // so no my_free in bitmap_free
    bitmap_free(&m_cols); // To pair with bitmap_init().
  }
}
size_t Rows_log_event::get_data_size()
{
  int const general_type_code= get_general_type_code();

  uchar buf[sizeof(m_width) + 1];
  uchar *end= net_store_length(buf, m_width);

  int data_size= 0;
  bool is_v2_event= common_header->type_code > binary_log::DELETE_ROWS_EVENT_V1;
  if (is_v2_event)
  {
    data_size= Binary_log_event::ROWS_HEADER_LEN_V2 +
      (m_extra_row_data ?
       ROWS_V_TAG_LEN + m_extra_row_data[EXTRA_ROW_INFO_LEN_OFFSET]:
       0);
  }
  else
  {
    data_size= Binary_log_event::ROWS_HEADER_LEN_V1;
  }
  data_size+= no_bytes_in_map(&m_cols);
  data_size+= (uint) (end - buf);

  if (general_type_code == binary_log::UPDATE_ROWS_EVENT)
    data_size+= no_bytes_in_map(&m_cols_ai);

  data_size+= (uint) (m_rows_cur - m_rows_buf);
  return data_size; 
}

Write_rows_log_event::Write_rows_log_event(const char *buf, uint event_len,
                                           const Format_description_event
                                           *description_event)
: binary_log::Rows_event(buf, event_len, description_event),
  Rows_log_event(buf, event_len, description_event),
  binary_log::Write_rows_event(buf, event_len, description_event)
{
}

/*
  pretty_print_str()
*/

#ifdef MYSQL_CLIENT
static void pretty_print_str(IO_CACHE* cache, const char* str, size_t len)
{
  const char* end = str + len;
  my_b_printf(cache, "\'");
  while (str < end)
  {
    char c;
    switch ((c=*str++)) {
    case '\n': my_b_printf(cache, "\\n"); break;
    case '\r': my_b_printf(cache, "\\r"); break;
    case '\\': my_b_printf(cache, "\\\\"); break;
    case '\b': my_b_printf(cache, "\\b"); break;
    case '\t': my_b_printf(cache, "\\t"); break;
    case '\'': my_b_printf(cache, "\\'"); break;
    case 0   : my_b_printf(cache, "\\0"); break;
    default:
      my_b_printf(cache, "%c", c);
      break;
    }
  }
  my_b_printf(cache, "\'");
}
#endif /* MYSQL_CLIENT */

#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)

static void clear_all_errors(THD *thd, Relay_log_info *rli)
{
  thd->is_slave_error = 0;
  thd->clear_error();
  rli->clear_error();
  if (rli->workers_array_initialized)
  {
    for(size_t i= 0; i < rli->get_worker_count(); i++)
    {
      rli->get_worker(i)->clear_error();
    }
  }
}

inline int idempotent_error_code(int err_code)
{
  int ret= 0;

  switch (err_code)
  {
    case 0:
      ret= 1;
    break;
    /*
      The following list of "idempotent" errors
      means that an error from the list might happen
      because of idempotent (more than once)
      applying of a binlog file.
      Notice, that binlog has a  ddl operation its
      second applying may cause

      case HA_ERR_TABLE_DEF_CHANGED:
      case HA_ERR_CANNOT_ADD_FOREIGN:

      which are not included into to the list.

      Note that HA_ERR_RECORD_DELETED is not in the list since
      do_exec_row() should not return that error code.
    */
    case HA_ERR_RECORD_CHANGED:
    case HA_ERR_KEY_NOT_FOUND:
    case HA_ERR_END_OF_FILE:
    case HA_ERR_FOUND_DUPP_KEY:
    case HA_ERR_FOUND_DUPP_UNIQUE:
    case HA_ERR_FOREIGN_DUPLICATE_KEY:
    case HA_ERR_NO_REFERENCED_ROW:
    case HA_ERR_ROW_IS_REFERENCED:
      ret= 1;
    break;
    default:
      ret= 0;
    break;
  }
  return (ret);
}

/**
  Ignore error code specified on command line.
*/

int ignored_error_code(int err_code)
{
  return ((err_code == ER_SLAVE_IGNORED_TABLE) ||
          (use_slave_mask && bitmap_is_set(&slave_error_mask, err_code)));
}

/*
  This function converts an engine's error to a server error.
   
  If the thread does not have an error already reported, it tries to 
  define it by calling the engine's method print_error. However, if a 
  mapping is not found, it uses the ER_UNKNOWN_ERROR and prints out a 
  warning message.
*/ 
int convert_handler_error(int error, THD* thd, TABLE *table)
{
  uint actual_error= (thd->is_error() ? thd->get_stmt_da()->mysql_errno() :
                           0);

  if (actual_error == 0)
  {
    table->file->print_error(error, MYF(0));
    actual_error= (thd->is_error() ? thd->get_stmt_da()->mysql_errno() :
                        ER_UNKNOWN_ERROR);
    if (actual_error == ER_UNKNOWN_ERROR)
      sql_print_warning("Unknown error detected %d in handler", error);
  }

  return (actual_error);
}

inline bool concurrency_error_code(int error)
{
  switch (error)
  {
  case ER_LOCK_WAIT_TIMEOUT:
  case ER_LOCK_DEADLOCK:
  case ER_XA_RBDEADLOCK:
    return TRUE;
  default: 
    return (FALSE);
  }
}

inline bool unexpected_error_code(int unexpected_error)
{
  switch (unexpected_error) 
  {
  case ER_NET_READ_ERROR:
  case ER_NET_ERROR_ON_WRITE:
  case ER_QUERY_INTERRUPTED:
  case ER_SERVER_SHUTDOWN:
  case ER_NEW_ABORTING_CONNECTION:
    return(TRUE);
  default:
    return(FALSE);
  }
}

/*
  pretty_print_str()
*/

static char *pretty_print_str(char *packet, const char *str, size_t len)
{
  const char *end= str + len;
  char *pos= packet;
  *pos++= '\'';
  while (str < end)
  {
    char c;
    switch ((c=*str++)) {
    case '\n': *pos++= '\\'; *pos++= 'n'; break;
    case '\r': *pos++= '\\'; *pos++= 'r'; break;
    case '\\': *pos++= '\\'; *pos++= '\\'; break;
    case '\b': *pos++= '\\'; *pos++= 'b'; break;
    case '\t': *pos++= '\\'; *pos++= 't'; break;
    case '\'': *pos++= '\\'; *pos++= '\''; break;
    case 0   : *pos++= '\\'; *pos++= '0'; break;
    default:
      *pos++= c;
      break;
    }
  }
  *pos++= '\'';
  return pos;
}
#endif /* !MYSQL_CLIENT */


#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)

/**
  Creates a temporary name for load data infile:.

  @param buf		      Store new filename here
  @param file_id	      File_id (part of file name)
  @param event_server_id     Event_id (part of file name)
  @param ext		      Extension for file name

  @return
    Pointer to start of extension
*/

static char *slave_load_file_stem(char *buf, uint file_id,
                                  int event_server_id, const char *ext)
{
  char *res;
  fn_format(buf,PREFIX_SQL_LOAD,slave_load_tmpdir, "", MY_UNPACK_FILENAME);
  to_unix_path(buf);

  buf= strend(buf);
  int appended_length= sprintf(buf, "%s-%d-", server_uuid, event_server_id);
  buf+= appended_length;
  res= int10_to_str(file_id, buf, 10);
  my_stpcpy(res, ext);                             // Add extension last
  return res;                                   // Pointer to extension
}
#endif


#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)

/**
  Delete all temporary files used for SQL_LOAD.
*/

static void cleanup_load_tmpdir()
{
  MY_DIR *dirp;
  FILEINFO *file;
  uint i;
  char fname[FN_REFLEN], prefbuf[TEMP_FILE_MAX_LEN], *p;

  if (!(dirp=my_dir(slave_load_tmpdir,MYF(0))))
    return;

  /* 
     When we are deleting temporary files, we should only remove
     the files associated with the server id of our server.
     We don't use event_server_id here because since we've disabled
     direct binlogging of Create_file/Append_file/Exec_load events
     we cannot meet Start_log event in the middle of events from one 
     LOAD DATA.
  */
  p= strmake(prefbuf, STRING_WITH_LEN(PREFIX_SQL_LOAD));
  sprintf(p,"%s-",server_uuid);

  for (i=0 ; i < dirp->number_off_files; i++)
  {
    file=dirp->dir_entry+i;
    if (is_prefix(file->name, prefbuf))
    {
      fn_format(fname,file->name,slave_load_tmpdir,"",MY_UNPACK_FILENAME);
      mysql_file_delete(key_file_misc, fname, MYF(0));
    }
  }

  my_dirend(dirp);
}
#endif


/*
  Stores string to IO_CACHE file.

  Writes str to file in the following format:
   1. Stores length using only one byte (255 maximum value);
   2. Stores complete str.
*/

static bool write_str_at_most_255_bytes(IO_CACHE *file, const char *str,
                                        uint length)
{
  /*
  uchar tmp[1];
  tmp[0]= (uchar) length;
  return (my_b_safe_write(file, tmp, sizeof(tmp)) ||
	  my_b_safe_write(file, (uchar*) str, length));
    */
   return true;
}

/**
  Transforms a string into "" or its expression in 0x... form.
*/

char *str_to_hex(char *to, const char *from, size_t len)
{
  if (len)
  {
    *to++= '0';
    *to++= 'x';
    to= octet2hex(to, from, len);
  }
  else
    to= stpcpy(to, "\"\"");
  return to;                               // pointer to end 0 of 'to'
}




/**
  Append a version of the 'from' string suitable for use in a query to
  the 'to' string.  To generate a correct escaping, the character set
  information in 'csinfo' is used.
*/
/*
int
append_query_string(THD *thd, const CHARSET_INFO *csinfo,
                    String const *from, String *to)
{
  char *beg, *ptr;
  size_t const orig_len= to->length();
  if (to->reserve(orig_len + from->length()*2+3))
    return 1;

  beg= to->c_ptr_quick() + to->length();
  ptr= beg;
  if (csinfo->escape_with_backslash_is_dangerous)
    ptr= str_to_hex(ptr, from->ptr(), from->length());
  else
  {
    *ptr++= '\'';
    
    if (!(thd->variables.sql_mode & MODE_NO_BACKSLASH_ESCAPES))
    {
      ptr+= escape_string_for_mysql(csinfo, ptr, 0,
                                    from->ptr(), from->length());
    }
    else
    {
    
      const char *frm_str= from->ptr();

      for (; frm_str < (from->ptr() + from->length()); frm_str++)
      {
       
        if (*frm_str == '\'')
          *ptr++= *frm_str;

        *ptr++= *frm_str;
      }
    }

    *ptr++= '\'';
  }
  to->length(orig_len + ptr - beg);
  return 0;
}
*/

/**
  Prints a "session_var=value" string. Used by mysqlbinlog to print some SET
  commands just before it prints a query.
*/

#ifdef MYSQL_CLIENT

static void print_set_option(IO_CACHE* file, uint32 bits_changed,
                             uint32 option, uint32 flags, const char* name,
                             bool* need_comma)
{
  if (bits_changed & option)
  {
    if (*need_comma)
      my_b_printf(file,", ");
    my_b_printf(file,"%s=%d", name, MY_TEST(flags & option));
    *need_comma= 1;
  }
}
#endif
/**************************************************************************
	Log_event methods (= the parent class of all events)
**************************************************************************/

/**
  @return
  returns the human readable name of the event's type
*/

const char* Log_event::get_type_str(Log_event_type type)
{
  switch(type) {
  case binary_log::START_EVENT_V3:  return "Start_v3";
  case binary_log::STOP_EVENT:   return "Stop";
  case binary_log::QUERY_EVENT:  return "Query";
  case binary_log::ROTATE_EVENT: return "Rotate";
  case binary_log::INTVAR_EVENT: return "Intvar";
  case binary_log::LOAD_EVENT:   return "Load";
  case binary_log::NEW_LOAD_EVENT:   return "New_load";
  case binary_log::CREATE_FILE_EVENT: return "Create_file";
  case binary_log::APPEND_BLOCK_EVENT: return "Append_block";
  case binary_log::DELETE_FILE_EVENT: return "Delete_file";
  case binary_log::EXEC_LOAD_EVENT: return "Exec_load";
  case binary_log::RAND_EVENT: return "RAND";
  case binary_log::XID_EVENT: return "Xid";
  case binary_log::USER_VAR_EVENT: return "User var";
  case binary_log::FORMAT_DESCRIPTION_EVENT: return "Format_desc";
  case binary_log::TABLE_MAP_EVENT: return "Table_map";
  case binary_log::PRE_GA_WRITE_ROWS_EVENT: return "Write_rows_event_old";
  case binary_log::PRE_GA_UPDATE_ROWS_EVENT: return "Update_rows_event_old";
  case binary_log::PRE_GA_DELETE_ROWS_EVENT: return "Delete_rows_event_old";
  case binary_log::WRITE_ROWS_EVENT_V1: return "Write_rows_v1";
  case binary_log::UPDATE_ROWS_EVENT_V1: return "Update_rows_v1";
  case binary_log::DELETE_ROWS_EVENT_V1: return "Delete_rows_v1";
  case binary_log::BEGIN_LOAD_QUERY_EVENT: return "Begin_load_query";
  case binary_log::EXECUTE_LOAD_QUERY_EVENT: return "Execute_load_query";
  case binary_log::INCIDENT_EVENT: return "Incident";
  case binary_log::IGNORABLE_LOG_EVENT: return "Ignorable";
  case binary_log::ROWS_QUERY_LOG_EVENT: return "Rows_query";
  case binary_log::WRITE_ROWS_EVENT: return "Write_rows";
  case binary_log::UPDATE_ROWS_EVENT: return "Update_rows";
  case binary_log::DELETE_ROWS_EVENT: return "Delete_rows";
  case binary_log::GTID_LOG_EVENT: return "Gtid";
  case binary_log::ANONYMOUS_GTID_LOG_EVENT: return "Anonymous_Gtid";
  case binary_log::PREVIOUS_GTIDS_LOG_EVENT: return "Previous_gtids";
  case binary_log::HEARTBEAT_LOG_EVENT: return "Heartbeat";
  case binary_log::TRANSACTION_CONTEXT_EVENT: return "Transaction_context";
  case binary_log::VIEW_CHANGE_EVENT: return "View_change";
  case binary_log::XA_PREPARE_LOG_EVENT: return "XA_prepare";
  default: return "Unknown";                            /* impossible */
  }
}

const char* Log_event::get_type_str()
{
  return get_type_str(get_type_code());
}


/*
  Log_event::Log_event()
*/

#ifndef MYSQL_CLIENT
/*
Log_event::Log_event(uint16 flags_arg,
                     enum_event_cache_type cache_type_arg,
                     enum_event_logging_type logging_type_arg,
                     Log_event_header *header, Log_event_footer *footer)
  : is_valid_param(false), temp_buf(0), exec_time(0),
    event_cache_type(cache_type_arg), event_logging_type(logging_type_arg),
    crc(0), common_header(header), common_footer(footer)
{
  server_id= thd->server_id;
  common_header->unmasked_server_id= server_id;
  common_header->when= thd->start_time;
  common_header->log_pos= 0;
  common_header->flags= flags_arg;
}
*/
/**
  This minimal constructor is for when you are not even sure that there
  is a valid THD. For example in the server when we are shutting down or
  flushing logs after receiving a SIGHUP (then we must write a Rotate to
  the binlog but we have no THD, so we need this minimal constructor).
*/
/*
Log_event::Log_event(Log_event_header* header, Log_event_footer *footer,
                     enum_event_cache_type cache_type_arg,
                     enum_event_logging_type logging_type_arg)
  : is_valid_param(false), temp_buf(0), exec_time(0), event_cache_type(cache_type_arg),
   event_logging_type(logging_type_arg), crc(0), common_header(header),
   common_footer(footer), thd(0)
{
  server_id=	::server_id;
  common_header->unmasked_server_id= server_id;
}
*/
#endif /* !MYSQL_CLIENT */


/*
  Log_event::Log_event()
*/

Log_event::Log_event(Log_event_header *header,
                     Log_event_footer *footer)
  : is_valid_param(false), temp_buf(0), exec_time(0),
    event_cache_type(EVENT_INVALID_CACHE),
    event_logging_type(EVENT_INVALID_LOGGING),
    crc(0), common_header(header), common_footer(footer)
{
#ifndef MYSQL_CLIENT
  //hd= 0;
#endif
  /*
     Mask out any irrelevant parts of the server_id
  */
#ifdef HAVE_REPLICATION
  server_id = common_header->unmasked_server_id & opt_server_id_mask;
#else
  server_id = common_header->unmasked_server_id;
#endif
}

/*
  This method is not on header file to avoid using key_memory_log_event
  outside log_event.cc, allowing header file to be included on plugins.
*/

void* Log_event::operator new(size_t size)
{
  //return my_malloc(key_memory_log_event, size, MYF(MY_WME|MY_FAE));
  return malloc(size);
}




/**
  init_show_field_list() prepares the column names and types for the
  output of SHOW BINLOG EVENTS; it is used only by SHOW BINLOG
  EVENTS.
*/
/*
void Log_event::init_show_field_list(List<Item>* field_list)
{
  field_list->push_back(new Item_empty_string("Log_name", 20));
  field_list->push_back(new Item_return_int("Pos", MY_INT32_NUM_DECIMAL_DIGITS,
					    MYSQL_TYPE_LONGLONG));
  field_list->push_back(new Item_empty_string("Event_type", 20));
  field_list->push_back(new Item_return_int("Server_id", 10,
					    MYSQL_TYPE_LONG));
  field_list->push_back(new Item_return_int("End_log_pos",
                                            MY_INT32_NUM_DECIMAL_DIGITS,
					    MYSQL_TYPE_LONGLONG));
  field_list->push_back(new Item_empty_string("Info", 20));
}
*/

/*
bool Log_event::wrapper_my_b_safe_write(IO_CACHE* file, const uchar* buf, size_t size)
{
  DBUG_EXECUTE_IF("simulate_temp_file_write_error",
                  {
                    file->write_pos=file->write_end;
                    DBUG_SET("+d,simulate_file_write_error");
                  });
  if (need_checksum() && size != 0)
    crc= checksum_crc32(crc, buf, size);
  bool ret = my_b_safe_write(file, buf, size);
  DBUG_EXECUTE_IF("simulate_temp_file_write_error",
                  {
                    DBUG_SET("-d,simulate_file_write_error");
                  });
  return ret;
}
*/
/*
bool Log_event::write_footer(IO_CACHE* file) 
{

  if (need_checksum())
  {
    uchar buf[BINLOG_CHECKSUM_LEN];
    int4store(buf, crc);
    return (my_b_safe_write(file, (uchar*) buf, sizeof(buf)));
  }
  return 0;
}
*/

uint32 Log_event::write_header_to_memory(uchar *buf)
{
  // Query start time
  //ulong timestamp= (ulong) get_time();
  ulong timestamp= 0;

  int4store(buf, timestamp);
  buf[EVENT_TYPE_OFFSET]= get_type_code();
  int4store(buf + SERVER_ID_OFFSET, server_id);
  int4store(buf + EVENT_LEN_OFFSET,
            static_cast<uint32>(common_header->data_written));
  int4store(buf + LOG_POS_OFFSET,
            static_cast<uint32>(common_header->log_pos));
  int2store(buf + FLAGS_OFFSET, common_header->flags);

  return LOG_EVENT_HEADER_LEN;
}


bool Log_event::write_header(IO_CACHE* file, size_t event_data_length)
{
  uchar header[LOG_EVENT_HEADER_LEN];
  bool ret = false;


  /* Store number of bytes that will be written by this event */
  common_header->data_written= event_data_length + sizeof(header);

  /*
  if (need_checksum())
  {
    crc= checksum_crc32(0L, NULL, 0);
    common_header->data_written += BINLOG_CHECKSUM_LEN;
  }
  */

  /*
    log_pos != 0 if this is relay-log event. In this case we should not
    change the position
  */

  if (is_artificial_event())
  {
    /*
      Artificial events are automatically generated and do not exist
      in master's binary log, so log_pos should be set to 0.
    */
    common_header->log_pos= 0;
  }
  else  if (!common_header->log_pos)
  {
    /*
      Calculate position of end of event

      Note that with a SEQ_READ_APPEND cache, my_b_tell() does not
      work well.  So this will give slightly wrong positions for the
      Format_desc/Rotate/Stop events which the slave writes to its
      relay log. For example, the initial Format_desc will have
      end_log_pos=91 instead of 95. Because after writing the first 4
      bytes of the relay log, my_b_tell() still reports 0. Because
      my_b_append() does not update the counter which my_b_tell()
      later uses (one should probably use my_b_append_tell() to work
      around this).  To get right positions even when writing to the
      relay log, we use the (new) my_b_safe_tell().

      Note that this raises a question on the correctness of all these
      DBUG_ASSERT(my_b_tell()=rli->event_relay_log_pos).

      If in a transaction, the log_pos which we calculate below is not
      very good (because then my_b_safe_tell() returns start position
      of the BEGIN, so it's like the statement was at the BEGIN's
      place), but it's not a very serious problem (as the slave, when
      it is in a transaction, does not take those end_log_pos into
      account (as it calls inc_event_relay_log_pos()). To be fixed
      later, so that it looks less strange. But not bug.
    */

    //common_header->log_pos= my_b_safe_tell(file) + common_header->data_written;
  }

  write_header_to_memory(header);

  //ret= my_b_safe_write(file, header, LOG_EVENT_HEADER_LEN);

  /*
    Update the checksum.

    In case this is a Format_description_log_event, we need to clear
    the LOG_EVENT_BINLOG_IN_USE_F flag before computing the checksum,
    since the flag will be cleared when the binlog is closed.  On
    verification, the flag is dropped before computing the checksum
    too.
  */
  if (
      (common_header->flags & LOG_EVENT_BINLOG_IN_USE_F) != 0)
  {
    common_header->flags &= ~LOG_EVENT_BINLOG_IN_USE_F;
    int2store(header + FLAGS_OFFSET, common_header->flags);
  }
  //crc= my_checksum(crc, header, LOG_EVENT_HEADER_LEN);

  //DBUG_RETURN( ret);
  return ret;
}




#ifndef MYSQL_CLIENT
#define UNLOCK_MUTEX if (log_lock) mysql_mutex_unlock(log_lock);
#define LOCK_MUTEX if (log_lock) mysql_mutex_lock(log_lock);
#else
#define UNLOCK_MUTEX
#define LOCK_MUTEX
#endif

Query_log_event::Query_log_event(const char* buf, uint event_len,
                                 const Format_description_event
                                 *description_event,
                                 Log_event_type event_type)
  :binary_log::Query_event(buf, event_len, description_event, event_type),
   Log_event(header(), footer())
{
  
  //slave_proxy_id= thread_id;
  exec_time= query_exec_time;

  ulong buf_len= catalog_len + 1 +
                  time_zone_len + 1 +
                  user_len + 1 +
                  host_len + 1 +
                  data_len + 1;
/*                  
#if !defined(MYSQL_CLIENT)
  buf_len+= sizeof(size_t) + db_len + 1 + QUERY_CACHE_FLAGS_SIZE;
#endif
*/

  if (!(data_buf = (Log_event_header::Byte*) malloc(buf_len)))
    return;
  /*
    The data buffer is used by the slave SQL thread while applying
    the event. The catalog, time_zone)str, user, host, db, query
    are pointers to this data_buf. The function call below, points these
    const pointers to the data buffer.
  */
  if (!(fill_data_buf(data_buf, buf_len)))
    return;

  if (query != 0 && q_len > 0)
    is_valid_param= true;

  /**
    The buffer contains the following:
    +--------+-----------+------+------+---------+----+-------+
    | catlog | time_zone | user | host | db name | \0 | Query |
    +--------+-----------+------+------+---------+----+-------+

    To support the query cache we append the following buffer to the above
    +-------+----------------------------------------+-------+
    |db len | uninitiatlized space of size of db len | FLAGS |
    +-------+----------------------------------------+-------+

    The area of buffer starting from Query field all the way to the end belongs
    to the Query buffer and its structure is described in alloc_query() in
    sql_parse.cc

    We append the db length at the end of the buffer. This will be used by
    Query_cache::send_result_to_client() in case the query cache is On.
   */
#if !defined(MYSQL_CLIENT)
  size_t db_length= db_len;
  memcpy(data_buf + query_data_written, &db_length, sizeof(size_t));
#endif
  
}

/**
  Binlog format tolerance is in (buf, event_len, description_event)
  constructors.
*/

Log_event* Log_event::read_log_event(const char* buf, uint event_len,
				     const char **error,
                                     const Format_description_log_event *description_event,
                                     my_bool crc_check)
{
  Log_event* ev= NULL;
  enum_binlog_checksum_alg  alg;

  /* Check the integrity */
  if (event_len < EVENT_LEN_OFFSET ||
      event_len != uint4korr(buf+EVENT_LEN_OFFSET))
  {
    *error="Sanity check failed";		// Needed to free buffer
    return NULL; // general sanity check - will fail on a partial read
  }

  uint event_type= buf[EVENT_TYPE_OFFSET];
  // all following START events in the current file are without checksum
  if (event_type == binary_log::START_EVENT_V3)
    (const_cast< Format_description_log_event *>(description_event))->
            common_footer->checksum_alg= binary_log::BINLOG_CHECKSUM_ALG_OFF;
  // Sanity check for Format description event
  if (event_type == binary_log::FORMAT_DESCRIPTION_EVENT)
  {
    if (event_len < LOG_EVENT_MINIMAL_HEADER_LEN +
        ST_COMMON_HEADER_LEN_OFFSET)
    {
      *error= "Found invalid Format description event in binary log";
      return 0;
    }
    uint tmp_header_len= buf[LOG_EVENT_MINIMAL_HEADER_LEN + ST_COMMON_HEADER_LEN_OFFSET];
    if (event_len < tmp_header_len + ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN)
    {
      *error= "Found invalid Format description event in binary log";
      return 0;
    }
  }
  /*
    CRC verification by SQL and Show-Binlog-Events master side.
    The caller has to provide @description_event->checksum_alg to
    be the last seen FD's (A) descriptor.
    If event is FD the descriptor is in it.
    Notice, FD of the binlog can be only in one instance and therefore
    Show-Binlog-Events executing master side thread needs just to know
    the only FD's (A) value -  whereas RL can contain more.
    In the RL case, the alg is kept in FD_e (@description_event) which is reset 
    to the newer read-out event after its execution with possibly new alg descriptor.
    Therefore in a typical sequence of RL:
    {FD_s^0, FD_m, E_m^1} E_m^1 
    will be verified with (A) of FD_m.

    See legends definition on MYSQL_BIN_LOG::relay_log_checksum_alg docs
    lines (log.h).

    Notice, a pre-checksum FD version forces alg := BINLOG_CHECKSUM_ALG_UNDEF.
  */
  alg= (event_type != binary_log::FORMAT_DESCRIPTION_EVENT) ?
       description_event->common_footer->checksum_alg :
       Log_event_footer::get_checksum_alg(buf, event_len);
  // Emulate the corruption during reading an event
  /*
  DBUG_EXECUTE_IF("corrupt_read_log_event_char",
    if (event_type != binary_log::FORMAT_DESCRIPTION_EVENT)
    {
      char *debug_event_buf_c = (char *)buf;
      int debug_cor_pos = rand() % (event_len - BINLOG_CHECKSUM_LEN);
      debug_event_buf_c[debug_cor_pos] =~ debug_event_buf_c[debug_cor_pos];
      DBUG_PRINT("info", ("Corrupt the event at Log_event::read_log_event(char*,...): byte on position %d", debug_cor_pos));
      DBUG_SET("");
    }
  );*/

  if (crc_check &&
      Log_event_footer::event_checksum_test((uchar *) buf, event_len, alg) )
  {
    *error= "Event crc check failed! Most likely there is event corruption.";
#ifdef MYSQL_CLIENT
    if (force_opt)
    {
      ev= new Unknown_log_event(buf, description_event);
      DBUG_RETURN(ev);
    }
#endif
    return NULL;
  }

  if (event_type > description_event->number_of_event_types &&
      event_type != binary_log::FORMAT_DESCRIPTION_EVENT )
  {
    /*
      It is unsafe to use the description_event if its post_header_len
      array does not include the event type.
    */

    ev= NULL;
  } 
  else
  {
    /*
      In some previuos versions (see comment in
      Format_description_log_event::Format_description_log_event(char*,...)),
      event types were assigned different id numbers than in the
      present version. In order to replicate from such versions to the
      present version, we must map those event type id's to our event
      type id's.  The mapping is done with the event_type_permutation
      array, which was set up when the Format_description_log_event
      was read.
    */
    if (description_event->event_type_permutation)
    {
      uint new_event_type;
      if (event_type >= EVENT_TYPE_PERMUTATION_NUM)
        /* Safe guard for read out of bounds of event_type_permutation. */
        new_event_type= binary_log::UNKNOWN_EVENT;
      else
        new_event_type= description_event->event_type_permutation[event_type];

      event_type= new_event_type;
    }

    if (alg != binary_log::BINLOG_CHECKSUM_ALG_UNDEF &&
        (event_type == binary_log::FORMAT_DESCRIPTION_EVENT ||
         alg != binary_log::BINLOG_CHECKSUM_ALG_OFF))
      event_len= event_len - BINLOG_CHECKSUM_LEN;

    switch(event_type) {
    case binary_log::QUERY_EVENT:
      ev  = new Query_log_event(buf, event_len, description_event,binary_log::QUERY_EVENT);
      break;
    case binary_log::LOAD_EVENT:
    case binary_log::NEW_LOAD_EVENT:

      ev = NULL;
      break;
    case binary_log::ROTATE_EVENT:
      //ev = NULL;
      ev = new Rotate_log_event(buf, event_len, description_event);
      break;/*
    case binary_log::CREATE_FILE_EVENT:
      ev = new Create_file_log_event(buf, event_len, description_event);
      break;*/
    case binary_log::APPEND_BLOCK_EVENT:
      ev = NULL;
      break;
    case binary_log::DELETE_FILE_EVENT:
      ev = NULL;
      break;
    case binary_log::EXEC_LOAD_EVENT:
      ev = NULL;
      break;
    case binary_log::START_EVENT_V3: /* this is sent only by MySQL <=4.x */
      //ev = new Start_log_event_v3(buf, event_len, description_event);
      break;
    case binary_log::STOP_EVENT:
      ev = NULL;
      break;
    case binary_log::INTVAR_EVENT:
      ev = NULL;
      break;
    case binary_log::XID_EVENT:
      ev = NULL;
      break;
    case binary_log::RAND_EVENT:
      ev = NULL;
      break;
    case binary_log::USER_VAR_EVENT:
      ev = NULL;
      break;
    case binary_log::FORMAT_DESCRIPTION_EVENT:
      //ev = new Format_description_log_event(buf, event_len, description_event);
      break;
    case binary_log::TABLE_MAP_EVENT:
      if (!(description_event->post_header_len.empty()))
        ev = new Table_map_log_event(buf, event_len, description_event);
      break;      
#if defined(HAVE_REPLICATION)
    case binary_log::PRE_GA_WRITE_ROWS_EVENT:
      ev = new Write_rows_log_event_old(buf, event_len, description_event);
      break;
    case binary_log::PRE_GA_UPDATE_ROWS_EVENT:
      ev = new Update_rows_log_event_old(buf, event_len, description_event);
      break;
    case binary_log::PRE_GA_DELETE_ROWS_EVENT:
      ev = new Delete_rows_log_event_old(buf, event_len, description_event);
      break;
    case binary_log::WRITE_ROWS_EVENT_V1:
      if (!(description_event->post_header_len.empty()))
        ev = new Write_rows_log_event(buf, event_len, description_event);
      break;
    case binary_log::UPDATE_ROWS_EVENT_V1:
      if (!(description_event->post_header_len.empty()))
        ev = new Update_rows_log_event(buf, event_len, description_event);
      break;
    case binary_log::DELETE_ROWS_EVENT_V1:
      if (!(description_event->post_header_len.empty()))
        ev = new Delete_rows_log_event(buf, event_len, description_event);
      break;

#endif
    case binary_log::BEGIN_LOAD_QUERY_EVENT:
      ev = NULL;
      break;
    case binary_log::EXECUTE_LOAD_QUERY_EVENT:
      ev = NULL;
      break;

    case binary_log::ROWS_QUERY_LOG_EVENT:
      ev= new Rows_query_log_event(buf, event_len, description_event);
      break;
    case binary_log::GTID_LOG_EVENT:
    case binary_log::ANONYMOUS_GTID_LOG_EVENT:
      ev= NULL;
      break;
    case binary_log::PREVIOUS_GTIDS_LOG_EVENT:
      ev = NULL;
      break;
    case binary_log::WRITE_ROWS_EVENT:
      ev = new Write_rows_log_event(buf, event_len, description_event);
      break;     
#if defined(HAVE_REPLICATION)      
    case binary_log::UPDATE_ROWS_EVENT:
      ev = new Update_rows_log_event(buf, event_len, description_event);
      break;
    case binary_log::DELETE_ROWS_EVENT:
      ev = new Delete_rows_log_event(buf, event_len, description_event);
      break;
    case binary_log::TRANSACTION_CONTEXT_EVENT:
      ev = new Transaction_context_log_event(buf, event_len, description_event);
      break;
    case binary_log::VIEW_CHANGE_EVENT:
      ev = new View_change_log_event(buf, event_len, description_event);
      break;
#endif
    case binary_log::XA_PREPARE_LOG_EVENT:
      ev = NULL;
      break;
    default:
      /*
        Create an object of Ignorable_log_event for unrecognized sub-class.
        So that SLAVE SQL THREAD will only update the position and continue.
      */
      if (uint2korr(buf + FLAGS_OFFSET) & LOG_EVENT_IGNORABLE_F)
      {
        //ev= new Ignorable_log_event(buf, description_event);
      }
      else
      {
        //DBUG_PRINT("error",("Unknown event code: %d",
        //                    (int) buf[EVENT_TYPE_OFFSET]));
        ev= NULL;
      }
      break;
    }
  }

  if (ev)
  {
    ev->common_footer->checksum_alg= alg;
    if (ev->common_footer->checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_OFF &&
        ev->common_footer->checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_UNDEF)
      ev->crc= uint4korr(buf + (event_len));
  }

  /*DBUG_PRINT("read_event", ("%s(type_code: %d; event_len: %d)",
                            ev ? ev->get_type_str() : "<unknown>",
                            buf[EVENT_TYPE_OFFSET],
                            event_len));*/
  /*
    is_valid is used for small event-specific sanity tests which are
    important; for example there are some my_malloc() in constructors
    (e.g. Query_log_event::Query_log_event(char*...)); when these
    my_malloc() fail we can't return an error out of the constructor
    (because constructor is "void") ; so instead we leave the pointer we
    wanted to allocate (e.g. 'query') to 0 and we test it and set the
    value of is_valid to true or false based on the test.
    Same for Format_description_log_event, member 'post_header_len'.

    SLAVE_EVENT is never used, so it should not be read ever.
  */
 
  if (!ev || !ev->is_valid() || (event_type == binary_log::SLAVE_EVENT))
  {
    //DBUG_PRINT("error",("Found invalid event in binary log"));
    delete ev;
#ifdef MYSQL_CLIENT
    if (!force_opt) 
    {
      *error= "Found invalid event in binary log";
      DBUG_RETURN(0);
    }
    ev= new Unknown_log_event(buf, description_event);
#else
    *error= "Found invalid event in binary log";
    
    return 0;
#endif
  }
  
  //DBUG_RETURN(ev);  
  return ev;
}
const char _my_bits_nbits[256] = {
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

static inline uint my_count_bits_uint32(uint32 v)
{
  return (uint) (uchar) (_my_bits_nbits[(uchar)  v] +
                         _my_bits_nbits[(uchar) (v >> 8)] +
                         _my_bits_nbits[(uchar) (v >> 16)] +
                         _my_bits_nbits[(uchar) (v >> 24)]);
}
uint bitmap_bits_set(const MY_BITMAP *map)
{
  my_bitmap_map *data_ptr= map->bitmap;
  my_bitmap_map *end= map->last_word_ptr;
  uint res= 0;

  for (; data_ptr < end; data_ptr++)
    res+= my_count_bits_uint32(*data_ptr);

  /*Reset last bits to zero*/
  res+= my_count_bits_uint32(*map->last_word_ptr & ~map->last_word_mask);
  return res;
}

static inline my_bool bitmap_is_set(const MY_BITMAP *map, uint bit)
{
  return ((uchar*)map->bitmap)[bit / 8] & (1 << (bit & 7));
}



static inline int32 sint4korr(const uchar *A) { return *((int32*) A); }
static inline int16  sint2korr(const uchar *A) { return *((int16*) A); }
static inline int32 sint3korr(const uchar *A)
{
  return
    ((int32) (((A[2]) & 128) ?
              (((uint32) 255L << 24) |
               (((uint32) A[2]) << 16) |
               (((uint32) A[1]) << 8) |
               ((uint32) A[0])) :
              (((uint32) A[2]) << 16) |
              (((uint32) A[1]) << 8) |
              ((uint32) A[0])))
    ;
}
static inline uint32 uint3korr(const uchar *A)
{
  return
    (uint32) (((uint32) (A[0])) +
              (((uint32) (A[1])) << 8) +
              (((uint32) (A[2])) << 16))
    ;
}
static inline longlong  sint8korr(const uchar *A) { return *((longlong*) A); }
static inline ulonglong uint8korr(const uchar *A) { return *((ulonglong*) A);}
static void my_b_write_sint32_and_uint32(int32 si, uint32 ui)
{
  printf( "%l", si);
  if (si < 0)
    printf( " (%u)", ui);
}

char *longlong10_to_str(longlong val,char *dst,int radix)
{
  char buffer[65];
  char *p;
  long long_val;
  ulonglong uval= (ulonglong) val;

  if (radix < 0)
  {
    if (val < 0)
    {
      *dst++ = '-';
      /* Avoid integer overflow in (-val) for LLONG_MIN (BUG#31799). */
      uval = (ulonglong)0 - uval;
    }
  }

  if (uval == 0)
  {
    *dst++='0';
    *dst='\0';
    return dst;
  }
  p = &buffer[sizeof(buffer)-1];
  *p = '\0';

  while (uval > (ulonglong) LONG_MAX)
  {
    ulonglong quo= uval/(uint) 10;
    uint rem= (uint) (uval- quo* (uint) 10);
    *--p = _dig_vec_upper[rem];
    uval= quo;
  }
  long_val= (long) uval;
  while (long_val != 0)
  {
    long quo= long_val/10;
    *--p = _dig_vec_upper[(uchar) (long_val - quo*10)];
    long_val= quo;
  }
  while ((*dst++ = *p++) != 0) ;
  return dst-1;
}

#define DIG_PER_DEC1 9
static const int dig2bytes[DIG_PER_DEC1+1]={0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

int decimal_bin_size(int precision, int scale)
{
  int intg=precision-scale,
      intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
      intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1;

  return intg0*sizeof(int32)+dig2bytes[intg0x]+
         frac0*sizeof(int32)+dig2bytes[frac0x];
}

inline int my_decimal_get_binary_size(uint precision, uint scale)
{
  return decimal_bin_size((int)precision, (int)scale);
}
static void
my_b_write_bit(const uchar *ptr, uint nbits)
{
  uint bitnum, nbits8= ((nbits + 7) / 8) * 8, skip_bits= nbits8 - nbits;
  printf( "b'");
  for (bitnum= skip_bits ; bitnum < nbits8; bitnum++)
  {
    int is_set= (ptr[(bitnum) / 8] >> (7 - bitnum % 8))  & 0x01;
    printf("%c", (const uchar*) (is_set ? "1" : "0"), 1);
  }
  printf("'");
}

static void
my_b_write_quoted(IO_CACHE *file, const uchar *ptr, uint length)
{
  const uchar *s;
  printf( "'");
  for (s= ptr; length > 0 ; s++, length--)
  {
    if (*s > 0x1F && *s != '\'' && *s != '\\')
      ;//my_b_write(file, s, 1);
    else
    {
      uchar hex[10];
      size_t len= snprintf((char*) hex, sizeof(hex), "%s%02x", "\\x", *s);
      //my_b_write(file, hex, len);
    }
  }
  printf( "'");
}

static size_t
my_b_write_quoted_with_length(IO_CACHE *file, const uchar *ptr, uint length)
{
  if (length < 256)
  {
    length= *ptr;
    my_b_write_quoted(file, ptr + 1, length);
    return length + 1;
  }
  else
  {
    length= uint2korr(ptr);
    my_b_write_quoted(file, ptr + 2, length);
    return length + 2;
  }
}
/**
  Print a packed value of the given SQL type into IO cache
  
  @param[in] file              IO cache
  @param[in] ptr               Pointer to string
  @param[in] type              Column type
  @param[in] meta              Column meta information
  @param[out] typestr          SQL type string buffer (for verbose output)
  @param[out] typestr_length   Size of typestr
  
  @retval   - number of bytes scanned from ptr.
*/
static size_t
log_event_print_value( const uchar *ptr,
                      uint type, uint meta,
                      char *typestr, size_t typestr_length)
{
  uint32 length= 0;

  if (type == MYSQL_TYPE_STRING_B)
  {
    if (meta >= 256)
    {
      uint byte0= meta >> 8;
      uint byte1= meta & 0xFF;
      
      if ((byte0 & 0x30) != 0x30)
      {
        /* a long CHAR() field: see #37426 */
        length= byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
        type= byte0 | 0x30;
      }
      else
        length = meta & 0xFF;
    }
    else
      length= meta;
  }

  switch (type) {
  case MYSQL_TYPE_LONG_B:
    {
      snprintf(typestr, typestr_length, "INT");
      if(!ptr)
        return printf("NULL");
      int32 si= sint4korr(ptr);
      uint32 ui= uint4korr(ptr);
      my_b_write_sint32_and_uint32(si, ui);
      return 4;
    }

  case MYSQL_TYPE_TINY_B:
    {
      snprintf(typestr, typestr_length, "TINYINT");
      if(!ptr)
        return printf("NULL");
      my_b_write_sint32_and_uint32((int) (signed char) *ptr,
                                  (uint) (unsigned char) *ptr);
      return 1;
    }

  case MYSQL_TYPE_SHORT_B:
    {
      snprintf(typestr, typestr_length, "SHORTINT");
      if(!ptr)
        return printf("NULL");
      int32 si= (int32) sint2korr(ptr);
      uint32 ui= (uint32) uint2korr(ptr);
      my_b_write_sint32_and_uint32( si, ui);
      return 2;
    }
  
  case MYSQL_TYPE_INT24_B:
    {
      snprintf(typestr, typestr_length, "MEDIUMINT");
      if(!ptr)
        return printf("NULL");
      int32 si= sint3korr(ptr);
      uint32 ui= uint3korr(ptr);
      my_b_write_sint32_and_uint32( si, ui);
      return 3;
    }

  case MYSQL_TYPE_LONGLONG_B:
    {
      snprintf(typestr, typestr_length, "LONGINT");
      if(!ptr)
        return printf("NULL");
      char tmp[64];
      longlong si= sint8korr(ptr);
      longlong10_to_str(si, tmp, -10);
      printf("%s", tmp);
      if (si < 0)
      {
        ulonglong ui= uint8korr(ptr);
        longlong10_to_str((longlong) ui, tmp, 10);
        printf(" (%s)", tmp);        
      }
      return 8;
    }

  /*
  case MYSQL_TYPE_NEWDECIMAL_B:
    {
      uint precision= meta >> 8;
      uint decimals= meta & 0xFF;
      snprintf(typestr, typestr_length, "DECIMAL(%d,%d)",
                  precision, decimals);
      if(!ptr)
        return printf("NULL");
      uint bin_size= my_decimal_get_binary_size(precision, decimals);
      my_decimal dec;
      binary2my_decimal(E_DEC_FATAL_ERROR, (uchar*) ptr, &dec,
                        precision, decimals);
      int len= DECIMAL_MAX_STR_LENGTH;
      char buff[DECIMAL_MAX_STR_LENGTH + 1];
      decimal2string(&dec,buff,&len, 0, 0, 0);
      my_b_printf(file, "%s", buff);
      return bin_size;
    }
    */
  /*
  case MYSQL_TYPE_FLOAT_B:
    {
      snprintf(typestr, typestr_length, "FLOAT");
      if(!ptr)
        return printf(file, "NULL");
      float fl;
      float4get(&fl, ptr);
      char tmp[320];
      sprintf(tmp, "%-20g", (double) fl);
      my_b_printf(file, "%s", tmp); 
      return 4;
    }
    

  case MYSQL_TYPE_DOUBLE_B:
    {
      strcpy(typestr, "DOUBLE");
      if(!ptr)
        return my_b_printf(file, "NULL");
      double dbl;
      float8get(&dbl, ptr);
      char tmp[320];
      sprintf(tmp, "%-.20g", dbl); 
      my_b_printf(file, "%s", tmp);
      return 8;
    }
  

  case MYSQL_TYPE_BIT_B:
    {
      
      uint nbits= ((meta >> 8) * 8) + (meta & 0xFF);
      my_snprintf(typestr, typestr_length, "BIT(%d)", nbits);
      if(!ptr)
        return my_b_printf(file, "NULL");
      length= (nbits + 7) / 8;
      my_b_write_bit(file, ptr, nbits);
      return length;
    }
    */

  case MYSQL_TYPE_TIMESTAMP_B:
    {
      snprintf(typestr, typestr_length, "TIMESTAMP");
      if(!ptr)
        return printf("NULL");
      uint32 i32= uint4korr(ptr);
      printf("%d", i32);
      return 4;
    }

 /*
  case MYSQL_TYPE_TIMESTAMP2_B:
    {
      snprintf(typestr, typestr_length, "TIMESTAMP(%d)", meta);
      if(!ptr)
        return printf("NULL");
      char buf[MAX_DATE_STRING_REP_LENGTH];
      struct timeval tm;
      my_timestamp_from_binary(&tm, ptr, meta);
      int buflen= my_timeval_to_str(&tm, buf, meta);
      my_b_write(file, buf, buflen);
      return my_timestamp_binary_length(meta);
    }
    */

  case MYSQL_TYPE_DATETIME_B:
    {
      snprintf(typestr, typestr_length, "DATETIME");
      if(!ptr)
        return printf("NULL");
      size_t d, t;
      uint64 i64= uint8korr(ptr); /* YYYYMMDDhhmmss */
      d= static_cast<size_t>(i64 / 1000000);
      t= i64 % 1000000;
      printf("%04d-%02d-%02d %02d:%02d:%02d",
                  static_cast<int>(d / 10000),
                  static_cast<int>(d % 10000) / 100,
                  static_cast<int>(d % 100),
                  static_cast<int>(t / 10000),
                  static_cast<int>(t % 10000) / 100,
                  static_cast<int>(t % 100));
      return 8;
    }
  
  /*
  case MYSQL_TYPE_DATETIME2_B:
    {
      my_snprintf(typestr, typestr_length, "DATETIME(%d)", meta);
      if(!ptr)
        return my_b_printf(file, "NULL");
      char buf[MAX_DATE_STRING_REP_LENGTH];
      MYSQL_TIME ltime;
      longlong packed= my_datetime_packed_from_binary(ptr, meta);
      TIME_from_longlong_datetime_packed(&ltime, packed);
      int buflen= my_datetime_to_str(&ltime, buf, meta);
      my_b_write_quoted(file, (uchar *) buf, buflen);
      return my_datetime_binary_length(meta);
    }
    */

  case MYSQL_TYPE_TIME_B:
    {
      snprintf(typestr, typestr_length, "TIME");
      if(!ptr)
        return printf( "NULL");
      uint32 i32= uint3korr(ptr);
      printf( "'%02d:%02d:%02d'",
                  i32 / 10000, (i32 % 10000) / 100, i32 % 100);
      return 3;
    }
  /*
  case MYSQL_TYPE_TIME2_B:
    {
      my_snprintf(typestr, typestr_length, "TIME(%d)", meta);
      if(!ptr)
        return my_b_printf(file, "NULL");
      char buf[MAX_DATE_STRING_REP_LENGTH];
      MYSQL_TIME ltime;
      longlong packed= my_time_packed_from_binary(ptr, meta);
      TIME_from_longlong_time_packed(&ltime, packed);
      int buflen= my_time_to_str(&ltime, buf, meta);
      my_b_write_quoted(file, (uchar *) buf, buflen);
      return my_time_binary_length(meta);
    }
   */
  case MYSQL_TYPE_NEWDATE_B:
    {
      snprintf(typestr, typestr_length, "DATE");
      if(!ptr)
        return printf( "NULL");
      uint32 tmp= uint3korr(ptr);
      int part;
      char buf[11];
      char *pos= &buf[10];  // start from '\0' to the beginning

      /* Copied from field.cc */
      *pos--=0;					// End NULL
      part=(int) (tmp & 31);
      *pos--= (char) ('0'+part%10);
      *pos--= (char) ('0'+part/10);
      *pos--= ':';
      part=(int) (tmp >> 5 & 15);
      *pos--= (char) ('0'+part%10);
      *pos--= (char) ('0'+part/10);
      *pos--= ':';
      part=(int) (tmp >> 9);
      *pos--= (char) ('0'+part%10); part/=10;
      *pos--= (char) ('0'+part%10); part/=10;
      *pos--= (char) ('0'+part%10); part/=10;
      *pos=   (char) ('0'+part);
      printf( "'%s'", buf);
      return 3;
    }

  case MYSQL_TYPE_YEAR_B:
    {
      snprintf(typestr, typestr_length, "YEAR");
      if(!ptr)
        return printf( "NULL");
      uint32 i32= *ptr;
      printf( "%04d", i32+ 1900);
      return 1;
    }
  
  case MYSQL_TYPE_ENUM_B:
    switch (meta & 0xFF) {
    case 1:
      snprintf(typestr, typestr_length, "ENUM(1 byte)");
      if(!ptr)
        return printf("NULL");
      printf( "%d", (int) *ptr);
      return 1;
    case 2:
      {
        snprintf(typestr, typestr_length, "ENUM(2 bytes)");
        if(!ptr)
          return printf("NULL");
        int32 i32= uint2korr(ptr);
        printf( "%d", i32);
        return 2;
      }
    default:
      printf("!! Unknown ENUM packlen=%d", meta & 0xFF); 
      return 0;
    }
    break;
    
  case MYSQL_TYPE_SET_B:
    snprintf(typestr, typestr_length, "SET(%d bytes)", meta & 0xFF);
    if(!ptr)
      return printf( "NULL");
    my_b_write_bit(ptr , (meta & 0xFF) * 8);
    return meta & 0xFF;

  /*
  case MYSQL_TYPE_BLOB_B:
    switch (meta) {
    case 1:
      snprintf(typestr, typestr_length, "TINYBLOB/TINYTEXT");
      if(!ptr)
        return printf("NULL");
      length= *ptr;
      my_b_write_quoted(file, ptr + 1, length);
      return length + 1;
    case 2:
      snprintf(typestr, typestr_length, "BLOB/TEXT");
      if(!ptr)
        return printf("NULL");
      length= uint2korr(ptr);
      my_b_write_quoted(file, ptr + 2, length);
      return length + 2;
    case 3:
      snprintf(typestr, typestr_length, "MEDIUMBLOB/MEDIUMTEXT");
      if(!ptr)
        return printf( "NULL");
      length= uint3korr(ptr);
      my_b_write_quoted(file, ptr + 3, length);
      return length + 3;
    case 4:
      snprintf(typestr, typestr_length, "LONGBLOB/LONGTEXT");
      if(!ptr)
        return printf("NULL");
      length= uint4korr(ptr);
      my_b_write_quoted(file, ptr + 4, length);
      return length + 4;
    default:
      printf( "!! Unknown BLOB packlen=%d", length);
      return 0;
    }*/

  case MYSQL_TYPE_VARCHAR_B:
  case MYSQL_TYPE_VAR_STRING_B:
    length= meta;
    snprintf(typestr, typestr_length, "VARSTRING(%d)", length);
    if(!ptr) 
      return printf( "NULL");
    return my_b_write_quoted_with_length(NULL, ptr, length);

  case MYSQL_TYPE_STRING_B:
    snprintf(typestr, typestr_length, "STRING(%d)", length);
    if(!ptr)
      return printf( "NULL");
    return my_b_write_quoted_with_length(NULL, ptr, length);

  case MYSQL_TYPE_JSON_B:
    snprintf(typestr, typestr_length, "JSON");
    if (!ptr)
      return printf( "NULL");
    length= uint2korr(ptr);
    my_b_write_quoted(NULL, ptr + meta, length);
    return length + meta;

  default:
    {
      char tmp[5];
      snprintf(tmp, sizeof(tmp), "%04x", meta);
      printf("!! Don't know how to handle column type=%d meta=%d (%s)",
                  type, meta, tmp);
    }
    break;
  }
  *typestr= 0;
  return 0;
}


size_t
Rows_log_event::print_verbose_one_row(table_def *td,MY_BITMAP *cols_bitmap,
                                      const uchar *value, const uchar *prefix)
{
  const uchar *value0= value;
  const uchar *null_bits= value;
  uint null_bit_index= 0;
  char typestr[64]= "";

  /*
    Skip metadata bytes which gives the information about nullabity of master
    columns. Master writes one bit for each affected column.
   */
  value+= (bitmap_bits_set(cols_bitmap) + 7) / 8;
  
  printf( "%s", prefix);
  
  for (size_t i= 0; i < td->size(); i ++)
  {
    int is_null= (null_bits[null_bit_index / 8] 
                  >> (null_bit_index % 8))  & 0x01;

    if (bitmap_is_set(cols_bitmap, i) == 0)
      continue;
    
    printf("###   @%d=", static_cast<int>(i + 1));
    if (!is_null)
    {
      size_t fsize= td->calc_field_size((uint)i, (uchar*) value);
      if (value + fsize > m_rows_end)
      {
        printf("***Corrupted replication event was detected."
                    " Not printing the value***\n");
        value+= fsize;
        return 0;
      }
    }
    size_t size= log_event_print_value(is_null? NULL: value,
                                         td->type(i), td->field_metadata(i),
                                         typestr, sizeof(typestr));
    if (!size)
      return 0;

    if(!is_null)
      value+= size;

    //if (print_event_info->verbose > 1)
    //{
      printf(" /* ");

      printf( "%s ", typestr);
      
      printf( "meta=%d nullable=%d is_null=%d ",
                  td->field_metadata(i),
                  td->maybe_null(i), is_null);
      printf( "*");
    //}
    
    printf( "\n");
    
    null_bit_index++;
  }
  
  return value - value0;
}

/**
  Print a row event into IO cache in human readable form (in SQL format)
  
  @param[in] file              IO cache
  @param[in] print_event_into  Print parameters
*/
//void Rows_log_event::print_verbose(IO_CACHE *file, PRINT_EVENT_INFO *print_event_info)
void Rows_log_event::print_verbose(Table_map_log_event *map)
{
  // Quoted length of the identifier can be twice the original length
  char quoted_db[1 + NAME_LEN * 2 + 2];
  char quoted_table[1 + NAME_LEN * 2 + 2];
  size_t quoted_db_len, quoted_table_len;
  //Table_map_log_event *map;
  table_def *td;
  const char *sql_command, *sql_clause1, *sql_clause2;
  Log_event_type general_type_code= get_general_type_code();
  
  if (m_extra_row_data)
  {
    uint8 extra_data_len= m_extra_row_data[EXTRA_ROW_INFO_LEN_OFFSET];
    uint8 extra_payload_len= extra_data_len - EXTRA_ROW_INFO_HDR_BYTES;
    assert(extra_data_len >= EXTRA_ROW_INFO_HDR_BYTES);

    printf("### Extra row data format: %u, len: %u :",
                m_extra_row_data[EXTRA_ROW_INFO_FORMAT_OFFSET],
                extra_payload_len);
    if (extra_payload_len)
    {
      /*
         Buffer for hex view of string, including '0x' prefix,
         2 hex chars / byte and trailing 0
      */
      const int buff_len= 2 + (256 * 2) + 1;
      char buff[buff_len];
      str_to_hex(buff, (const char*) &m_extra_row_data[EXTRA_ROW_INFO_HDR_BYTES],
                 extra_payload_len);
      printf("%s", buff);
    }
    printf("\n");
  }

  switch (general_type_code) {
  case binary_log::WRITE_ROWS_EVENT:
    sql_command= "INSERT INTO";
    sql_clause1= "### SET\n";
    sql_clause2= NULL;
    break;
  case binary_log::DELETE_ROWS_EVENT:
    sql_command= "DELETE FROM";
    sql_clause1= "### WHERE\n";
    sql_clause2= NULL;
    break;
  case binary_log::UPDATE_ROWS_EVENT:
    sql_command= "UPDATE";
    sql_clause1= "### WHERE\n";
    sql_clause2= "### SET\n";
    break;
  default:
    sql_command= sql_clause1= sql_clause2= NULL;
    return; /* Not possible */
  }
  
  td= map->create_table_def();
  /*
  if (!(map= print_event_info->m_table_map.get_table(m_table_id)) ||
      !(td= map->create_table_def()))
  {
    char llbuff[22];
    my_b_printf(file, "### Row event for unknown table #%s",
                llstr(m_table_id, llbuff));
    return;
  }
  */

  /* If the write rows event contained no values for the AI */
  if (((general_type_code == binary_log::WRITE_ROWS_EVENT) &&
      (m_rows_buf==m_rows_end)))
  {
    //printf("### INSERT INTO `%s`.`%s` VALUES ()\n", 
    //                  map->get_db_name(), map->get_table_name());
    printf("### INSERT INTO  VALUES ()\n");
    goto end;
  }

  for (const uchar *value= m_rows_buf; value < m_rows_end; )
  {
    size_t length;
    /*
#ifdef MYSQL_SERVER
    quoted_db_len= my_strmov_quoted_identifier(this->thd, (char *) quoted_db,
                                        map->get_db_name(), 0);
    quoted_table_len= my_strmov_quoted_identifier(this->thd,
                                                  (char *) quoted_table,
                                                  map->get_table_name(), 0);
#else
    quoted_db_len= my_strmov_quoted_identifier((char *) quoted_db,
                                               map->get_db_name());
    quoted_table_len= my_strmov_quoted_identifier((char *) quoted_table,
                                          map->get_table_name());
#endif
    quoted_db[quoted_db_len]= '\0';
    quoted_table[quoted_table_len]= '\0';
    printf("### %s %s.%s\n",
                      sql_command,
                      quoted_db, quoted_table);
                      */
    /* Print the first image */
    if (!(length= print_verbose_one_row(td,&m_cols, value,(const uchar*) sql_clause1)))
      goto end;
    value+= length;

    /* Print the second image (for UPDATE only) */
    if (sql_clause2)
    {
      if (!(length= print_verbose_one_row(td, &m_cols_ai, value,
                                      (const uchar*) sql_clause2)))
        goto end;
      value+= length;
    }
  }

end:
   return;
}


#ifdef MYSQL_CLIENT

/*
  Log_event::print_header()
*/

void Log_event::print_header(IO_CACHE* file,
                             PRINT_EVENT_INFO* print_event_info,
                             bool is_more MY_ATTRIBUTE((unused)))
{
  char llbuff[22];
  my_off_t hexdump_from= print_event_info->hexdump_from;
  DBUG_ENTER("Log_event::print_header");

  my_b_printf(file, "#");
  print_timestamp(file, NULL);
  my_b_printf(file, " server id %lu  end_log_pos %s ", (ulong) server_id,
              llstr(common_header->log_pos,llbuff));

  /* print the checksum */

  if (common_footer->checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_OFF &&
      common_footer->checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_UNDEF)
  {
    char checksum_buf[BINLOG_CHECKSUM_LEN * 2 + 4]; // to fit to "0x%lx "
    size_t const bytes_written=
      my_snprintf(checksum_buf, sizeof(checksum_buf), "0x%08lx ", (ulong) crc);
    my_b_printf(file, "%s ", get_type(&binlog_checksum_typelib,
                                      common_footer->checksum_alg));
    my_b_printf(file, checksum_buf, bytes_written);
  }

  /* mysqlbinlog --hexdump */
  if (print_event_info->hexdump_from)
  {
    my_b_printf(file, "\n");
    uchar *ptr= (uchar*)temp_buf;
    my_off_t size=
      uint4korr(ptr + EVENT_LEN_OFFSET) - LOG_EVENT_MINIMAL_HEADER_LEN;
    my_off_t i;

    /* Header len * 4 >= header len * (2 chars + space + extra space) */
    char *h, hex_string[49]= {0};
    char *c, char_string[16+1]= {0};

    /* Pretty-print event common header if header is exactly 19 bytes */
    if (print_event_info->common_header_len == LOG_EVENT_MINIMAL_HEADER_LEN)
    {
      char emit_buf[256];               // Enough for storing one line
      my_b_printf(file, "# Position  Timestamp   Type   Master ID        "
                  "Size      Master Pos    Flags \n");
      size_t const bytes_written=
        my_snprintf(emit_buf, sizeof(emit_buf),
                    "# %8.8lx %02x %02x %02x %02x   %02x   "
                    "%02x %02x %02x %02x   %02x %02x %02x %02x   "
                    "%02x %02x %02x %02x   %02x %02x\n",
                    (unsigned long) hexdump_from,
                    ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6],
                    ptr[7], ptr[8], ptr[9], ptr[10], ptr[11], ptr[12], ptr[13],
                    ptr[14], ptr[15], ptr[16], ptr[17], ptr[18]);
      DBUG_ASSERT(static_cast<size_t>(bytes_written) < sizeof(emit_buf));
      my_b_write(file, (uchar*) emit_buf, bytes_written);
      ptr += LOG_EVENT_MINIMAL_HEADER_LEN;
      hexdump_from += LOG_EVENT_MINIMAL_HEADER_LEN;
    }

    /* Rest of event (without common header) */
    for (i= 0, c= char_string, h=hex_string;
	 i < size;
	 i++, ptr++)
    {
      my_snprintf(h, 4, (i % 16 <= 7) ? "%02x " : " %02x", *ptr);
      h += 3;

      *c++= my_isalnum(&my_charset_bin, *ptr) ? *ptr : '.';

      if (i % 16 == 15)
      {
        /*
          my_b_printf() does not support full printf() formats, so we
          have to do it this way.

          TODO: Rewrite my_b_printf() to support full printf() syntax.
         */
        char emit_buf[256];
        size_t const bytes_written=
          my_snprintf(emit_buf, sizeof(emit_buf),
                      "# %8.8lx %-48.48s |%16s|\n",
                      (unsigned long) (hexdump_from + (i & 0xfffffff0)),
                      hex_string, char_string);
        DBUG_ASSERT(static_cast<size_t>(bytes_written) < sizeof(emit_buf));
	my_b_write(file, (uchar*) emit_buf, bytes_written);
	hex_string[0]= 0;
	char_string[0]= 0;
	c= char_string;
	h= hex_string;
      }
    }
    *c= '\0';
    DBUG_ASSERT(hex_string[48] == 0);
    
    if (hex_string[0])
    {
      char emit_buf[256];
      // Right-pad hex_string with spaces, up to 48 characters.
      memset(h, ' ', (sizeof(hex_string) -1) - (h - hex_string));
      size_t const bytes_written=
        my_snprintf(emit_buf, sizeof(emit_buf),
                    "# %8.8lx %-48.48s |%s|\n",
                    (unsigned long) (hexdump_from + (i & 0xfffffff0)),
                    hex_string, char_string);
      DBUG_ASSERT(static_cast<size_t>(bytes_written) < sizeof(emit_buf));
      my_b_write(file, (uchar*) emit_buf, bytes_written);
    }
    /*
      need a # to prefix the rest of printouts for example those of
      Rows_log_event::print_helper().
    */
    my_b_write(file, reinterpret_cast<const uchar*>("# "), 2);
  }
  DBUG_VOID_RETURN;
}


/**
  Prints a quoted string to io cache.
  Control characters are displayed as hex sequence, e.g. \x00
  
  @param[in] file              IO cache
  @param[in] prt               Pointer to string
  @param[in] length            String length
*/

static void
my_b_write_quoted(IO_CACHE *file, const uchar *ptr, uint length)
{
  const uchar *s;
  my_b_printf(file, "'");
  for (s= ptr; length > 0 ; s++, length--)
  {
    if (*s > 0x1F && *s != '\'' && *s != '\\')
      my_b_write(file, s, 1);
    else
    {
      uchar hex[10];
      size_t len= my_snprintf((char*) hex, sizeof(hex), "%s%02x", "\\x", *s);
      my_b_write(file, hex, len);
    }
  }
  my_b_printf(file, "'");
}

/**
  Prints a bit string to io cache in format  b'1010'.
  
  @param[in] file              IO cache
  @param[in] ptr               Pointer to string
  @param[in] nbits             Number of bits
*/
static void
my_b_write_bit(IO_CACHE *file, const uchar *ptr, uint nbits)
{
  uint bitnum, nbits8= ((nbits + 7) / 8) * 8, skip_bits= nbits8 - nbits;
  my_b_printf(file, "b'");
  for (bitnum= skip_bits ; bitnum < nbits8; bitnum++)
  {
    int is_set= (ptr[(bitnum) / 8] >> (7 - bitnum % 8))  & 0x01;
    my_b_write(file, (const uchar*) (is_set ? "1" : "0"), 1);
  }
  my_b_printf(file, "'");
}


/**
  Prints a packed string to io cache.
  The string consists of length packed to 1 or 2 bytes,
  followed by string data itself.
  
  @param[in] file              IO cache
  @param[in] ptr               Pointer to string
  @param[in] length            String size
  
  @retval   - number of bytes scanned.
*/
static size_t
my_b_write_quoted_with_length(IO_CACHE *file, const uchar *ptr, uint length)
{
  if (length < 256)
  {
    length= *ptr;
    my_b_write_quoted(file, ptr + 1, length);
    return length + 1;
  }
  else
  {
    length= uint2korr(ptr);
    my_b_write_quoted(file, ptr + 2, length);
    return length + 2;
  }
}


/**
  Prints a 32-bit number in both signed and unsigned representation
  
  @param[in] file              IO cache
  @param[in] sl                Signed number
  @param[in] ul                Unsigned number
*/
static void
my_b_write_sint32_and_uint32(IO_CACHE *file, int32 si, uint32 ui)
{
  my_b_printf(file, "%d", si);
  if (si < 0)
    my_b_printf(file, " (%u)", ui);
}




#ifdef MYSQL_CLIENT
void free_table_map_log_event(Table_map_log_event *event)
{
  delete event;
}
#endif

void Log_event::print_base64(IO_CACHE* file,
                             PRINT_EVENT_INFO* print_event_info,
                             bool more)
{
  const uchar *ptr= (const uchar *)temp_buf;
  uint32 size= uint4korr(ptr + EVENT_LEN_OFFSET);
  DBUG_ENTER("Log_event::print_base64");

  uint64 const tmp_str_sz= base64_needed_encoded_length((uint64) size);
  char *const tmp_str= (char *) my_malloc(key_memory_log_event,
                                          tmp_str_sz, MYF(MY_WME));
  if (!tmp_str) {
    fprintf(stderr, "\nError: Out of memory. "
            "Could not print correct binlog event.\n");
    DBUG_VOID_RETURN;
  }

  if (base64_encode(ptr, (size_t) size, tmp_str))
  {
    DBUG_ASSERT(0);
  }

  if (print_event_info->base64_output_mode != BASE64_OUTPUT_DECODE_ROWS)
  {
    if (my_b_tell(file) == 0)
      my_b_printf(file, "\nBINLOG '\n");

    my_b_printf(file, "%s\n", tmp_str);

    if (!more)
      my_b_printf(file, "'%s\n", print_event_info->delimiter);
  }
  
  if (print_event_info->verbose)
  {
    Rows_log_event *ev= NULL;
    Log_event_type et= (Log_event_type) ptr[EVENT_TYPE_OFFSET];

    if (common_footer->checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_UNDEF &&
        common_footer->checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_OFF)
      size-= BINLOG_CHECKSUM_LEN; // checksum is displayed through the header

    const Format_description_event fd_evt=
          Format_description_event(glob_description_event->binlog_version,
                                   server_version);
    switch(et)
    {
    case binary_log::TABLE_MAP_EVENT:
    {
      Table_map_log_event *map;
      map= new Table_map_log_event((const char*) ptr, size,
                                   &fd_evt);
      print_event_info->m_table_map.set_table(map->get_table_id(), map);
      break;
    }
    case binary_log::WRITE_ROWS_EVENT:
    case binary_log::WRITE_ROWS_EVENT_V1:
    {
      ev= new Write_rows_log_event((const char*) ptr, size,
                                   &fd_evt);
      break;
    }
    case binary_log::DELETE_ROWS_EVENT:
    case binary_log::DELETE_ROWS_EVENT_V1:
    {
      ev= new Delete_rows_log_event((const char*) ptr, size,
                                    &fd_evt);
      break;
    }
    case binary_log::UPDATE_ROWS_EVENT:
    case binary_log::UPDATE_ROWS_EVENT_V1:
    {
      ev= new Update_rows_log_event((const char*) ptr, size,
                                    &fd_evt);
      break;
    }
    default:
      break;
    }
    
    if (ev)
    {
      ev->print_verbose(&print_event_info->footer_cache, print_event_info);
      delete ev;
    }
  }
    
  free(tmp_str);
  DBUG_VOID_RETURN;
}


/*
  Log_event::print_timestamp()
*/

void Log_event::print_timestamp(IO_CACHE* file, time_t *ts)
{
  struct tm *res;
  /*
    In some Windows versions timeval.tv_sec is defined as "long",
    not as "time_t" and can be of a different size.
    Let's use a temporary time_t variable to execute localtime()
    with a correct argument type.
  */
  time_t ts_tmp= ts ? *ts : (ulong)common_header->when.tv_sec;
  DBUG_ENTER("Log_event::print_timestamp");
  struct tm tm_tmp;
  localtime_r(&ts_tmp, (res= &tm_tmp));
  my_b_printf(file,"%02d%02d%02d %2d:%02d:%02d",
              res->tm_year % 100,
              res->tm_mon+1,
              res->tm_mday,
              res->tm_hour,
              res->tm_min,
              res->tm_sec);
  DBUG_VOID_RETURN;
}

#endif /* MYSQL_CLIENT */


#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
inline Log_event::enum_skip_reason
Log_event::continue_group(Relay_log_info *rli)
{
  if (rli->slave_skip_counter == 1)
    return Log_event::EVENT_SKIP_IGNORE;
  return Log_event::do_shall_skip(rli);
}

/**
   @param end_group_sets_max_dbs  when true the group terminal event 
                          can carry partition info, see a note below.
   @return true  in cases the current event
                 carries partition data,
           false otherwise

   @note Some events combination may force to adjust partition info.
         In particular BEGIN, BEGIN_LOAD_QUERY_EVENT, COMMIT
         where none of the events holds partitioning data
         causes the sequential applying of the group through
         assigning OVER_MAX_DBS_IN_EVENT_MTS to mts_accessed_dbs
         of the group terminator (e.g COMMIT query) event.
*/
bool Log_event::contains_partition_info(bool end_group_sets_max_dbs)
{
  bool res;

  switch (get_type_code()) {
  case binary_log::TABLE_MAP_EVENT:
  case binary_log::EXECUTE_LOAD_QUERY_EVENT:
    res= true;

    break;

  case binary_log::QUERY_EVENT:
  {
    Query_log_event *qev= static_cast<Query_log_event*>(this);
    if ((ends_group() && end_group_sets_max_dbs) ||
        (qev->is_query_prefix_match(STRING_WITH_LEN("XA COMMIT")) ||
         qev->is_query_prefix_match(STRING_WITH_LEN("XA ROLLBACK"))))
    {
      res= true;
      qev->mts_accessed_dbs= OVER_MAX_DBS_IN_EVENT_MTS;
    }
    else
      res= (!ends_group() && !starts_group()) ? true : false;
    break;
  }
  default:
    res= false;
  }

  return res;
}
/*
  SYNOPSIS
    This function assigns a parent ID to the job group being scheduled in parallel.
    It also checks if we can schedule the new event in parallel with the previous ones
    being executed.

  @param        ev log event that has to be scheduled next.
  @param       rli Pointer to coordinato's relay log info.
  @return      true if error
               false otherwise
 */
bool schedule_next_event(Log_event* ev, Relay_log_info* rli)
{
  int error;
  // Check if we can schedule this event
  error= rli->current_mts_submode->schedule_next_event(rli, ev);
  switch (error)
  {
  case ER_MTS_CANT_PARALLEL:
    char llbuff[22];
    llstr(rli->get_event_relay_log_pos(), llbuff);
    my_error(ER_MTS_CANT_PARALLEL, MYF(0),
    ev->get_type_str(), rli->get_event_relay_log_name(), llbuff,
             "The master event is logically timestamped incorrectly.");
    return true;
  case ER_MTS_INCONSISTENT_DATA:
    /* Don't have to do anything. */
    return true;
  case -1:
    /* Unable to schedule: wait_for_last_committed_trx has failed */
    return true;
  default:
    return false;
  }
  /* Keep compiler happy */
  return false;
}


/**
   The method maps the event to a Worker and return a pointer to it.
   Sending the event to the Worker is done by the caller.

   Irrespective of the type of Group marking (DB partioned or BGC) the
   following holds true:

   - recognize the beginning of a group to allocate the group descriptor
     and queue it;
   - associate an event with a Worker (which also handles possible conflicts
     detection and waiting for their termination);
   - finalize the group assignement when the group closing event is met.

   When parallelization mode is BGC-based the partitioning info in the event
   is simply ignored. Thereby association with a Worker does not require
   Assigned Partition Hash of the partitioned method.
   This method is not interested in all the taxonomy of the event group
   property, what we care about is the boundaries of the group.

   As a part of the group, an event belongs to one of the following types:

   B - beginning of a group of events (BEGIN query_log_event)
   g - mini-group representative event containing the partition info
      (any Table_map, a Query_log_event)
   p - a mini-group internal event that *p*receeding its g-parent
      (int_, rand_, user_ var:s)
   r - a mini-group internal "regular" event that follows its g-parent
      (Delete, Update, Write -rows)
   T - terminator of the group (XID, COMMIT, ROLLBACK, auto-commit query)

   Only the first g-event computes the assigned Worker which once
   is determined remains to be for the rest of the group.
   That is the g-event solely carries partitioning info.
   For B-event the assigned Worker is NULL to indicate Coordinator
   has not yet decided. The same applies to p-event.

   Notice, these is a special group consisting of optionally multiple p-events
   terminating with a g-event.
   Such case is caused by old master binlog and a few corner-cases of
   the current master version (todo: to fix).

   In case of the event accesses more than OVER_MAX_DBS the method
   has to ensure sure previously assigned groups to all other workers are
   done.


   @note The function updates GAQ queue directly, updates APH hash
         plus relocates some temporary tables from Coordinator's list into
         involved entries of APH through @c map_db_to_worker.
         There's few memory allocations commented where to be freed.

   @return a pointer to the Worker struct or NULL.
*/

Slave_worker *Log_event::get_slave_worker(Relay_log_info *rli)
{
  Slave_job_group group= Slave_job_group(), *ptr_group= NULL;
  bool is_s_event;
  Slave_worker *ret_worker= NULL;
  char llbuff[22];
  Slave_committed_queue *gaq= rli->gaq;
  DBUG_ENTER("Log_event::get_slave_worker");

  /* checking partioning properties and perform corresponding actions */

  // Beginning of a group designated explicitly with BEGIN or GTID
  if ((is_s_event= starts_group()) || is_gtid_event(this) ||
      // or DDL:s or autocommit queries possibly associated with own p-events
      (!rli->curr_group_seen_begin && !rli->curr_group_seen_gtid &&
       /*
         the following is a special case of B-free still multi-event group like
         { p_1,p_2,...,p_k, g }.
         In that case either GAQ is empty (the very first group is being
         assigned) or the last assigned group index points at one of
         mapped-to-a-worker.
       */
       (gaq->empty() ||
        gaq->get_job_group(rli->gaq->assigned_group_index)->
        worker_id != MTS_WORKER_UNDEF)))
  {
    if (!rli->curr_group_seen_gtid && !rli->curr_group_seen_begin)
    {
      rli->mts_groups_assigned++;

      rli->curr_group_isolated= FALSE;
      group.reset(common_header->log_pos, rli->mts_groups_assigned);
      // the last occupied GAQ's array index
      gaq->assigned_group_index= gaq->en_queue(&group);
      DBUG_PRINT("info",("gaq_idx= %ld  gaq->size=%ld",
                         gaq->assigned_group_index,
                         gaq->size));
      DBUG_ASSERT(gaq->assigned_group_index != MTS_WORKER_UNDEF);
      DBUG_ASSERT(gaq->assigned_group_index < gaq->size);
      DBUG_ASSERT(gaq->get_job_group(rli->gaq->assigned_group_index)->
                  group_relay_log_name == NULL);
      DBUG_ASSERT(rli->last_assigned_worker == NULL ||
                  !is_mts_db_partitioned(rli));

      if (is_s_event || is_gtid_event(this))
      {
        Slave_job_item job_item= {this, rli->get_event_relay_log_number(),
                                  rli->get_event_start_pos()};
        // B-event is appended to the Deferred Array associated with GCAP
        rli->curr_group_da.push_back(job_item);

        DBUG_ASSERT(rli->curr_group_da.size() == 1);

        if (starts_group())
        {
          // mark the current group as started with explicit B-event
          rli->mts_end_group_sets_max_dbs= true;
          rli->curr_group_seen_begin= true;
        }

        if (is_gtid_event(this))
          // mark the current group as started with explicit Gtid-event
          rli->curr_group_seen_gtid= true;
        if (schedule_next_event(this, rli))
        {
          rli->abort_slave= 1;
          DBUG_RETURN(NULL);
        }
        DBUG_RETURN(ret_worker);
      }
    }
    else
    {
      /*
       The block is a result of not making GTID event as group starter.
       TODO: Make GITD event as B-event that is starts_group() to
       return true.
      */
      Slave_job_item job_item= {this, rli->get_event_relay_log_number(),
                                rli->get_event_relay_log_pos()};

      // B-event is appended to the Deferred Array associated with GCAP
      rli->curr_group_da.push_back(job_item);
      rli->curr_group_seen_begin= true;
      rli->mts_end_group_sets_max_dbs= true;
      if (!rli->curr_group_seen_gtid && schedule_next_event(this, rli))
      {
        rli->abort_slave= 1;
        DBUG_RETURN(NULL);
      }

      DBUG_ASSERT(rli->curr_group_da.size() == 2);
      DBUG_ASSERT(starts_group());
      DBUG_RETURN (ret_worker);
    }
    if (schedule_next_event(this, rli))
    {
      rli->abort_slave= 1;
      DBUG_RETURN(NULL);
    }
  }

  ptr_group= gaq->get_job_group(rli->gaq->assigned_group_index);
  if (!is_mts_db_partitioned(rli))
  {
    /* Get least occupied worker */
    ret_worker=
      rli->current_mts_submode->get_least_occupied_worker(rli, &rli->workers,
                                                          this);
    if (ret_worker == NULL)
    {
      /* get_least_occupied_worker may return NULL if the thread is killed */
      Slave_job_item job_item= {this, rli->get_event_relay_log_number(),
                                rli->get_event_start_pos()};
      rli->curr_group_da.push_back(job_item);

      DBUG_ASSERT(thd->killed);
      DBUG_RETURN(NULL);
    }
    ptr_group->worker_id= ret_worker->id;
  }
  else if (contains_partition_info(rli->mts_end_group_sets_max_dbs))
  {
    int i= 0;
    Mts_db_names mts_dbs;

    get_mts_dbs(&mts_dbs);
    /*
      Bug 12982188 - MTS: SBR ABORTS WITH ERROR 1742 ON LOAD DATA
      Logging on master can create a group with no events holding
      the partition info.
      The following assert proves there's the only reason
      for such group.
    */
#ifndef DBUG_OFF
    {
      bool empty_group_with_gtids= rli->curr_group_seen_begin &&
                                   rli->curr_group_seen_gtid &&
                                   ends_group();

      bool begin_load_query_event=
        ((rli->curr_group_da.size() == 3 && rli->curr_group_seen_gtid) ||
         (rli->curr_group_da.size() == 2 && !rli->curr_group_seen_gtid)) &&
        (rli->curr_group_da.back().data->
         get_type_code() == binary_log::BEGIN_LOAD_QUERY_EVENT);

      bool delete_file_event=
        ((rli->curr_group_da.size() == 4 && rli->curr_group_seen_gtid) ||
         (rli->curr_group_da.size() == 3 && !rli->curr_group_seen_gtid)) &&
        (rli->curr_group_da.back().data->
         get_type_code() == binary_log::DELETE_FILE_EVENT);

      DBUG_ASSERT((!ends_group() ||
                   (get_type_code() == binary_log::QUERY_EVENT &&
                    static_cast<Query_log_event*>(this)->
                    is_query_prefix_match(STRING_WITH_LEN("XA ROLLBACK")))) ||
                  empty_group_with_gtids ||
                  (rli->mts_end_group_sets_max_dbs &&
                   (begin_load_query_event || delete_file_event)));
    }
#endif

    // partioning info is found which drops the flag
    rli->mts_end_group_sets_max_dbs= false;
    ret_worker= rli->last_assigned_worker;
    if (mts_dbs.num == OVER_MAX_DBS_IN_EVENT_MTS)
    {
      // Worker with id 0 to handle serial execution
      if (!ret_worker)
        ret_worker= rli->workers.at(0);
      // No need to know a possible error out of synchronization call.
      (void) rli->current_mts_submode->
        wait_for_workers_to_finish(rli, ret_worker);
      /*
        this marking is transferred further into T-event of the current group.
      */
      rli->curr_group_isolated= TRUE;
    }

    /* One run of the loop in the case of over-max-db:s */
    for (i= 0; i < ((mts_dbs.num != OVER_MAX_DBS_IN_EVENT_MTS) ? mts_dbs.num : 1);
         i++)
    {
      /*
        The over max db:s case handled through passing to map_db_to_worker
        such "all" db as encoded as  the "" empty string.
        Note, the empty string is allocated in a large buffer
        to satisfy hashcmp() implementation.
      */
      const char all_db[NAME_LEN]= {0};
      if (!(ret_worker=
            map_db_to_worker(mts_dbs.num == OVER_MAX_DBS_IN_EVENT_MTS ?
                             all_db : mts_dbs.name[i], rli,
                             &mts_assigned_partitions[i],
                             /*
                               todo: optimize it. Although pure
                               rows- event load in insensetive to the flag value
                             */
                             TRUE,
                             ret_worker)))
      {
        llstr(rli->get_event_relay_log_pos(), llbuff);
        my_error(ER_MTS_CANT_PARALLEL, MYF(0),
                 get_type_str(), rli->get_event_relay_log_name(), llbuff,
                 "could not distribute the event to a Worker");
        DBUG_RETURN(ret_worker);
      }
      // all temporary tables are transferred from Coordinator in over-max case
      DBUG_ASSERT(mts_dbs.num != OVER_MAX_DBS_IN_EVENT_MTS || !thd->temporary_tables);
      DBUG_ASSERT(!strcmp(mts_assigned_partitions[i]->db,
                          mts_dbs.num != OVER_MAX_DBS_IN_EVENT_MTS ?
                          mts_dbs.name[i] : all_db));
      DBUG_ASSERT(ret_worker == mts_assigned_partitions[i]->worker);
      DBUG_ASSERT(mts_assigned_partitions[i]->usage >= 0);
    }

    if ((ptr_group= gaq->get_job_group(rli->gaq->assigned_group_index))->
        worker_id == MTS_WORKER_UNDEF)
    {
      ptr_group->worker_id= ret_worker->id;

      DBUG_ASSERT(ptr_group->group_relay_log_name == NULL);
    }

    DBUG_ASSERT(i == mts_dbs.num || mts_dbs.num == OVER_MAX_DBS_IN_EVENT_MTS);
  }
  else
  {
    // a mini-group internal "regular" event
    if (rli->last_assigned_worker)
    {
      ret_worker= rli->last_assigned_worker;

      DBUG_ASSERT(rli->curr_group_assigned_parts.size() > 0 ||
                  ret_worker->id == 0);
    }
    else // int_, rand_, user_ var:s, load-data events
    {

      if (!(get_type_code() == binary_log::INTVAR_EVENT ||
            get_type_code() == binary_log::RAND_EVENT ||
            get_type_code() == binary_log::USER_VAR_EVENT ||
            get_type_code() == binary_log::BEGIN_LOAD_QUERY_EVENT ||
            get_type_code() == binary_log::APPEND_BLOCK_EVENT ||
            get_type_code() == binary_log::DELETE_FILE_EVENT ||
            is_ignorable_event()))
      {
        DBUG_ASSERT(!ret_worker);

        llstr(rli->get_event_relay_log_pos(), llbuff);
        my_error(ER_MTS_CANT_PARALLEL, MYF(0),
                 get_type_str(), rli->get_event_relay_log_name(), llbuff,
                 "the event is a part of a group that is unsupported in "
                 "the parallel execution mode");

        DBUG_RETURN(ret_worker);
      }
      /*
        In the logical clock scheduler any internal gets scheduled directly.
        That is Int_var, @User_var and Rand bypass the deferred array.
        Their association with relay-log physical coordinates is provided
        by the same mechanism that applies to a regular event.
      */
      Slave_job_item job_item= {this, rli->get_event_relay_log_number(),
                                rli->get_event_start_pos()};
      rli->curr_group_da.push_back(job_item);

      DBUG_ASSERT(!ret_worker);
      DBUG_RETURN (ret_worker);
    }
  }

  DBUG_ASSERT(ret_worker);
  // T-event: Commit, Xid, a DDL query or dml query of B-less group.4

  /*
    Preparing event physical coordinates info for Worker before any
    event got scheduled so when Worker error-stopped at the first
    event it would be aware of where exactly in the event stream.
  */
  if (!ret_worker->master_log_change_notified)
  {
    if (!ptr_group)
      ptr_group= gaq->get_job_group(rli->gaq->assigned_group_index);
    ptr_group->group_master_log_name=
      my_strdup(key_memory_log_event, rli->get_group_master_log_name(), MYF(MY_WME));
    ret_worker->master_log_change_notified= true;

    DBUG_ASSERT(!ptr_group->notified);
#ifndef DBUG_OFF
    ptr_group->notified= true;
#endif
  }

  /* Notify the worker about new FD */
  if (!ret_worker->fd_change_notified)
  {
    if (!ptr_group)
      ptr_group= gaq->get_job_group(rli->gaq->assigned_group_index);
    /*
      Increment the usage counter on behalf of Worker.
      This avoids inadvertent FD deletion in a race case where Coordinator
      would install a next new FD before Worker has noticed the previous one.
    */
    rli->get_rli_description_event()->usage_counter.atomic_add(1);
    ptr_group->new_fd_event= rli->get_rli_description_event();
    ret_worker->fd_change_notified= true;
  }

  if (ends_group() ||
      (!rli->curr_group_seen_begin &&
       (get_type_code() == binary_log::QUERY_EVENT ||
        /*
          When applying an old binary log without Gtid_log_event and
          Anonymous_gtid_log_event, the logic of multi-threaded slave
          still need to require that an event (for example, Query_log_event,
          User_var_log_event, Intvar_log_event, and Rand_log_event) that
          appeared outside of BEGIN...COMMIT was treated as a transaction
          of its own. This was just a technicality in the code and did not
          cause a problem, since the event and the following Query_log_event
          would both be assigned to dedicated worker 0.
        */
        !rli->curr_group_seen_gtid)))
  {
    rli->mts_group_status= Relay_log_info::MTS_END_GROUP;
    if (rli->curr_group_isolated)
      set_mts_isolate_group();
    if (!ptr_group)
      ptr_group= gaq->get_job_group(rli->gaq->assigned_group_index);

    DBUG_ASSERT(ret_worker != NULL);

    /*
      The following two blocks are executed if the worker has not been
      notified about new relay-log or a new checkpoints.
      Relay-log string is freed by Coordinator, Worker deallocates
      strings in the checkpoint block.
      However if the worker exits earlier reclaiming for both happens anyway at
      GAQ delete.
    */
    if (!ret_worker->relay_log_change_notified)
    {
      /*
        Prior this event, C rotated the relay log to drop each
        Worker's notified flag. Now group terminating event initiates
        the new relay-log (where the current event is from) name
        delivery to Worker that will receive it in commit_positions().
      */
      DBUG_ASSERT(ptr_group->group_relay_log_name == NULL);

      ptr_group->group_relay_log_name= (char *)
        my_malloc(key_memory_log_event,
                  strlen(rli->
                         get_group_relay_log_name()) + 1, MYF(MY_WME));
      strcpy(ptr_group->group_relay_log_name,
             rli->get_event_relay_log_name());

      DBUG_ASSERT(ptr_group->group_relay_log_name != NULL);

      ret_worker->relay_log_change_notified= TRUE;
    }

    if (!ret_worker->checkpoint_notified)
    {
      if (!ptr_group)
        ptr_group= gaq->get_job_group(rli->gaq->assigned_group_index);
      ptr_group->checkpoint_log_name=
        my_strdup(key_memory_log_event, rli->get_group_master_log_name(), MYF(MY_WME));
      ptr_group->checkpoint_log_pos= rli->get_group_master_log_pos();
      ptr_group->checkpoint_relay_log_name=
        my_strdup(key_memory_log_event, rli->get_group_relay_log_name(), MYF(MY_WME));
      ptr_group->checkpoint_relay_log_pos= rli->get_group_relay_log_pos();
      ptr_group->shifted= ret_worker->bitmap_shifted;
      ret_worker->bitmap_shifted= 0;
      ret_worker->checkpoint_notified= TRUE;
    }
    ptr_group->checkpoint_seqno= rli->checkpoint_seqno;
    ptr_group->ts= common_header->when.tv_sec + (time_t) exec_time; // Seconds_behind_master related
    rli->checkpoint_seqno++;
    /*
      Coordinator should not use the main memroot however its not
      reset elsewhere either, so let's do it safe way.
      The main mem root is also reset by the SQL thread in at the end
      of applying which Coordinator does not do in this case.
      That concludes the memroot reset can't harm anything in SQL thread roles
      after Coordinator has finished its current scheduling.
    */
    free_root(thd->mem_root,MYF(MY_KEEP_PREALLOC));

#ifndef DBUG_OFF
    w_rr++;
#endif

  }

  DBUG_RETURN (ret_worker);
}


int Log_event::apply_gtid_event(Relay_log_info *rli)
{
  DBUG_ENTER("LOG_EVENT:apply_gtid_event");

  int error= 0;
  if (rli->curr_group_da.size() < 1)
    DBUG_RETURN(1);

  Log_event* ev= rli->curr_group_da[0].data;
  DBUG_ASSERT(ev->get_type_code() == binary_log::GTID_LOG_EVENT ||
              ev->get_type_code() ==
              binary_log::ANONYMOUS_GTID_LOG_EVENT);

  error= ev->do_apply_event(rli);
  /* Clean up */
  delete ev;
  rli->curr_group_da.clear();
  rli->curr_group_seen_gtid= false;
  /*
    Removes the job from the (G)lobal (A)ssigned (Q)ueue after
    applying it.
  */
  DBUG_ASSERT(rli->gaq->len > 0);
  Slave_job_group g= Slave_job_group();
  rli->gaq->de_tail(&g);
  /*
    The rli->mts_groups_assigned is increased when adding the slave job
    generated for the gtid into the (G)lobal (A)ssigned (Q)ueue. So we
    decrease it here.
  */
  rli->mts_groups_assigned--;

  DBUG_RETURN(error);
}


/**
   Scheduling event to execute in parallel or execute it directly.
   In MTS case the event gets associated with either Coordinator or a
   Worker.  A special case of the association is NULL when the Worker
   can't be decided yet.  In the single threaded sequential mode the
   event maps to SQL thread rli.

   @note in case of MTS failure Coordinator destroys all gathered
         deferred events.

   @return 0 as success, otherwise a failure.
*/
int Log_event::apply_event(Relay_log_info *rli)
{
  DBUG_ENTER("LOG_EVENT:apply_event");
  DBUG_PRINT("info", ("event_type=%s", get_type_str()));
  bool parallel= FALSE;
  enum enum_mts_event_exec_mode actual_exec_mode= EVENT_EXEC_PARALLEL;
  THD *rli_thd= rli->info_thd;

  worker= rli;

  if (rli->is_mts_recovery())
  {
    bool skip=
      bitmap_is_set(&rli->recovery_groups, rli->mts_recovery_index) &&
      (get_mts_execution_mode(::server_id,
                              rli->mts_group_status ==
                              Relay_log_info::MTS_IN_GROUP,
                              rli->current_mts_submode->get_type() ==
                              MTS_PARALLEL_TYPE_DB_NAME)
       == EVENT_EXEC_PARALLEL);
    if (skip)
    {
      DBUG_RETURN(0);
    }
    else
    {
      DBUG_RETURN(do_apply_event(rli));
    }
  }

  if (!(parallel= rli->is_parallel_exec()) ||
      ((actual_exec_mode=
        get_mts_execution_mode(::server_id,
                               rli->mts_group_status ==
                               Relay_log_info::MTS_IN_GROUP,
                               rli->current_mts_submode->get_type() ==
                               MTS_PARALLEL_TYPE_DB_NAME))
       != EVENT_EXEC_PARALLEL))
  {
    if (parallel)
    {
      /*
         There are two classes of events that Coordinator executes
         itself. One e.g the master Rotate requires all Workers to finish up
         their assignments. The other async class, e.g the slave Rotate,
         can't have this such synchronization because Worker might be waiting
         for terminal events to finish.
      */

      if (actual_exec_mode != EVENT_EXEC_ASYNC)
      {
        /*
          this  event does not split the current group but is indeed
          a separator beetwen two master's binlog therefore requiring
          Workers to sync.
        */
        if (rli->curr_group_da.size() > 0 &&
            is_mts_db_partitioned(rli) &&
            get_type_code() != binary_log::INCIDENT_EVENT)
        {
          char llbuff[22];
          /*
             Possible reason is a old version binlog sequential event
             wrappped with BEGIN/COMMIT or preceeded by User|Int|Random- var.
             MTS has to stop to suggest restart in the permanent sequential mode.
          */
          llstr(rli->get_event_relay_log_pos(), llbuff);
          my_error(ER_MTS_CANT_PARALLEL, MYF(0),
                   get_type_str(), rli->get_event_relay_log_name(), llbuff,
                   "possible malformed group of events from an old master");

          /* Coordinator cant continue, it marks MTS group status accordingly */
          rli->mts_group_status= Relay_log_info::MTS_KILLED_GROUP;

          goto err;
        }

        if (get_type_code() == binary_log::INCIDENT_EVENT &&
            rli->curr_group_da.size() > 0 &&
            rli->current_mts_submode->get_type() ==
            MTS_PARALLEL_TYPE_LOGICAL_CLOCK)
        {
#ifndef DBUG_OFF
          DBUG_ASSERT(rli->curr_group_da.size() == 1);
          Log_event* ev= rli->curr_group_da[0].data;
          DBUG_ASSERT(ev->get_type_code() == binary_log::GTID_LOG_EVENT ||
                      ev->get_type_code() ==
                      binary_log::ANONYMOUS_GTID_LOG_EVENT);
#endif
          /*
            With MTS logical clock mode, when coordinator is applying an
            incident event, it must withdraw delegated_job increased by
            the incident's GTID before waiting for workers to finish.
            So that it can exit from mts_checkpoint_routine.
          */
          ((Mts_submode_logical_clock*)rli->current_mts_submode)->
            withdraw_delegated_job();
        }
        /*
          Marking sure the event will be executed in sequential mode.
        */
        if (rli->current_mts_submode->wait_for_workers_to_finish(rli) == -1)
        {
          // handle synchronization error
          rli->report(WARNING_LEVEL, 0,
                      "Slave worker thread has failed to apply an event. As a "
                      "consequence, the coordinator thread is stopping "
                      "execution.");
          DBUG_RETURN(-1);
        }
        /*
          Given not in-group mark the event handler can invoke checkpoint
          update routine in the following course.
        */
        DBUG_ASSERT(rli->mts_group_status == Relay_log_info::MTS_NOT_IN_GROUP
                    || !is_mts_db_partitioned(rli));

        if (get_type_code() == binary_log::INCIDENT_EVENT &&
            rli->curr_group_da.size() > 0)
        {
          DBUG_ASSERT(rli->curr_group_da.size() == 1);
          /*
            When MTS is enabled, the incident event must be applied by the
            coordinator. So the coordinator applies its GTID right before
            applying the incident event..
          */
          int error= apply_gtid_event(rli);
          if (error)
            DBUG_RETURN(-1);
        }

#ifndef DBUG_OFF
        /* all Workers are idle as done through wait_for_workers_to_finish */
        for (uint k= 0; k < rli->curr_group_da.size(); k++)
        {
          DBUG_ASSERT(!(rli->workers[k]->usage_partition));
          DBUG_ASSERT(!(rli->workers[k]->jobs.len));
        }
#endif
      }
      else
      {
        DBUG_ASSERT(actual_exec_mode == EVENT_EXEC_ASYNC);
      }
    }
    DBUG_RETURN(do_apply_event(rli));
  }

  DBUG_ASSERT(actual_exec_mode == EVENT_EXEC_PARALLEL);
  DBUG_ASSERT(!(rli->curr_group_seen_begin && ends_group()) ||
              /*
                This is an empty group being processed due to gtids.
              */
              (rli->curr_group_seen_begin && rli->curr_group_seen_gtid
              && ends_group()) || is_mts_db_partitioned(rli) ||
              rli->last_assigned_worker ||
              /*
                Begin_load_query can be logged w/o db info and within
                Begin/Commit. That's a pattern forcing sequential
                applying of LOAD-DATA.
              */
              (rli->curr_group_da.back().data->
               get_type_code() == binary_log::BEGIN_LOAD_QUERY_EVENT) ||
              /*
                Delete_file can also be logged w/o db info and within
                Begin/Commit. That's a pattern forcing sequential
                applying of LOAD-DATA.
              */
              (rli->curr_group_da.back().data->
               get_type_code() == binary_log::DELETE_FILE_EVENT));

  worker= NULL;
  rli->mts_group_status= Relay_log_info::MTS_IN_GROUP;

  worker= (Relay_log_info*)
    (rli->last_assigned_worker= get_slave_worker(rli));

#ifndef DBUG_OFF
  if (rli->last_assigned_worker)
    DBUG_PRINT("mts", ("Assigning job to worker %lu",
               rli->last_assigned_worker->id));
#endif

err:
  if (rli_thd->is_error() || (!worker && rli->abort_slave))
  {
    DBUG_ASSERT(!worker);

    /*
      Destroy all deferred buffered events but the current prior to exit.
      The current one will be deleted as an event never destined/assigned
      to any Worker in Coordinator's regular execution path.
    */
    for (uint k= 0; k < rli->curr_group_da.size(); k++)
    {
      Log_event *ev_buf= rli->curr_group_da[k].data;
      if (this != ev_buf)
        delete ev_buf;
    }
    rli->curr_group_da.clear();
  }
  else
  {
    DBUG_ASSERT(worker || rli->curr_group_assigned_parts.size() == 0);
  }

  DBUG_RETURN((!(rli_thd->is_error() || (!worker && rli->abort_slave)) ||
               DBUG_EVALUATE_IF("fault_injection_get_slave_worker", 1, 0)) ?
              0 : -1);
}

#endif

/**************************************************************************
	Query_log_event methods
**************************************************************************/

#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)

/**
  This (which is used only for SHOW BINLOG EVENTS) could be updated to
  print SET @@session_var=. But this is not urgent, as SHOW BINLOG EVENTS is
  only an information, it does not produce suitable queries to replay (for
  example it does not print LOAD DATA INFILE).
  @todo
    show the catalog ??
*/

int Query_log_event::pack_info(Protocol *protocol)
{
  // TODO: show the catalog ??
  String str_buf;
  // Add use `DB` to the string if required
  if (!(common_header->flags & LOG_EVENT_SUPPRESS_USE_F)
      && db && db_len)
  {
    str_buf.append("use ");
    append_identifier(this->thd, &str_buf, db, db_len);
    str_buf.append("; ");
  }
  // Add the query to the string
  if (query && q_len)
    str_buf.append(query);
 // persist the buffer in protocol
  protocol->store(str_buf.ptr(), str_buf.length(), &my_charset_bin);
  return 0;
}
#endif

#ifndef MYSQL_CLIENT

/**
  Utility function for the next method (Query_log_event::write()) .
*/
static void write_str_with_code_and_len(uchar **dst, const char *src,
                                        size_t len, uint code)
{
  /*
    only 1 byte to store the length of catalog, so it should not
    surpass 255
  */
  //DBUG_ASSERT(len <= 255);
  //DBUG_ASSERT(src);
  *((*dst)++)= code;
  *((*dst)++)= (uchar) len;
  memmove(*dst, src, len);
  (*dst)+= len;
}


/**
  Query_log_event::write().

  @note
    In this event we have to modify the header to have the correct
    EVENT_LEN_OFFSET as we don't yet know how many status variables we
    will print!
*/

bool Query_log_event::write(IO_CACHE* file)
{
    return 1;
}


/**
  The simplest constructor that could possibly work.  This is used for
  creating static objects that have a special meaning and are invisible
  to the log.
*/
Query_log_event::Query_log_event()
  : binary_log::Query_event(),
    Log_event(header(), footer()),
    data_buf(NULL)
{}

/**
  Creates a Query Log Event.

  @param thd_arg      Thread handle
  @param query_arg    Array of char representing the query
  @param query_length Size of the 'query_arg' array
  @param using_trans  Indicates that there are transactional changes.
  @param immediate    After being written to the binary log, the event
                      must be flushed immediately. This indirectly implies
                      the stmt-cache.
  @param suppress_use Suppress the generation of 'USE' statements
  @param errcode      The error code of the query
  @param ignore       Ignore user's statement, i.e. lex information, while
                      deciding which cache must be used.
*/

#endif /* MYSQL_CLIENT */


/**
  This is used by the SQL slave thread to prepare the event before execution.
*/



#ifdef MYSQL_CLIENT
/**
  Query_log_event::print().

  @todo
    print the catalog ??
*/
void Query_log_event::print_query_header(IO_CACHE* file,
					 PRINT_EVENT_INFO* print_event_info)
{
  // TODO: print the catalog ??
  char buff[48], *end;  // Enough for "SET TIMESTAMP=1305535348.123456"
  char quoted_id[1+ 2*FN_REFLEN+ 2];
  size_t quoted_len= 0;
  bool different_db= true;
  uint32 tmp;

  if (!print_event_info->short_form)
  {
    print_header(file, print_event_info, FALSE);
    my_b_printf(file, "\t%s\tthread_id=%lu\texec_time=%lu\terror_code=%d\n",
                get_type_str(), (ulong) thread_id, (ulong) exec_time,
                error_code);
  }

  if ((common_header->flags & LOG_EVENT_SUPPRESS_USE_F))
  {
    if (!is_trans_keyword())
      print_event_info->db[0]= '\0';
  }
  else if (db)
  {
#ifdef MYSQL_SERVER
    quoted_len= my_strmov_quoted_identifier(this->thd, (char*)quoted_id, db, 0);
#else
    quoted_len= my_strmov_quoted_identifier((char*)quoted_id, db);
#endif
    quoted_id[quoted_len]= '\0';
    different_db= memcmp(print_event_info->db, db, db_len + 1);
    if (different_db)
      memcpy(print_event_info->db, db, db_len + 1);
    if (db[0] && different_db) 
      my_b_printf(file, "use %s%s\n", quoted_id, print_event_info->delimiter);
  }

  end= int10_to_str((long)common_header->when.tv_sec,
                    my_stpcpy(buff,"SET TIMESTAMP="),10);
  if (common_header->when.tv_usec)
    end+= sprintf(end, ".%06d", (int) common_header->when.tv_usec);
  end= my_stpcpy(end, print_event_info->delimiter);
  *end++='\n';
  DBUG_ASSERT(end < buff + sizeof(buff));
  my_b_write(file, (uchar*) buff, (uint) (end-buff));
  if ((!print_event_info->thread_id_printed ||
       ((common_header->flags & LOG_EVENT_THREAD_SPECIFIC_F) &&
        thread_id != print_event_info->thread_id)))
  {
    // If --short-form, print deterministic value instead of pseudo_thread_id.
    my_b_printf(file,"SET @@session.pseudo_thread_id=%lu%s\n",
                short_form ? 999999999 : (ulong)thread_id,
                print_event_info->delimiter);
    print_event_info->thread_id= thread_id;
    print_event_info->thread_id_printed= 1;
  }

  /*
    If flags2_inited==0, this is an event from 3.23 or 4.0; nothing to
    print (remember we don't produce mixed relay logs so there cannot be
    5.0 events before that one so there is nothing to reset).
  */
  if (likely(flags2_inited)) /* likely as this will mainly read 5.0 logs */
  {
    /* tmp is a bitmask of bits which have changed. */
    if (likely(print_event_info->flags2_inited)) 
      /* All bits which have changed */
      tmp= (print_event_info->flags2) ^ flags2;
    else /* that's the first Query event we read */
    {
      print_event_info->flags2_inited= 1;
      tmp= ~((uint32)0); /* all bits have changed */
    }

    if (unlikely(tmp)) /* some bits have changed */
    {
      bool need_comma= 0;
      my_b_printf(file, "SET ");
      print_set_option(file, tmp, OPTION_NO_FOREIGN_KEY_CHECKS, ~flags2,
                       "@@session.foreign_key_checks", &need_comma);
      print_set_option(file, tmp, OPTION_AUTO_IS_NULL, flags2,
                       "@@session.sql_auto_is_null", &need_comma);
      print_set_option(file, tmp, OPTION_RELAXED_UNIQUE_CHECKS, ~flags2,
                       "@@session.unique_checks", &need_comma);
      print_set_option(file, tmp, OPTION_NOT_AUTOCOMMIT, ~flags2,
                       "@@session.autocommit", &need_comma);
      my_b_printf(file,"%s\n", print_event_info->delimiter);
      print_event_info->flags2= flags2;
    }
  }

  /*
    Now the session variables;
    it's more efficient to pass SQL_MODE as a number instead of a
    comma-separated list.
    FOREIGN_KEY_CHECKS, SQL_AUTO_IS_NULL, UNIQUE_CHECKS are session-only
    variables (they have no global version; they're not listed in
    sql_class.h), The tests below work for pure binlogs or pure relay
    logs. Won't work for mixed relay logs but we don't create mixed
    relay logs (that is, there is no relay log with a format change
    except within the 3 first events, which mysqlbinlog handles
    gracefully). So this code should always be good.
  */

  if (likely(sql_mode_inited) &&
      (unlikely(print_event_info->sql_mode != sql_mode ||
                !print_event_info->sql_mode_inited)))
  {
    my_b_printf(file,"SET @@session.sql_mode=%lu%s\n",
                (ulong)sql_mode, print_event_info->delimiter);
    print_event_info->sql_mode= sql_mode;
    print_event_info->sql_mode_inited= 1;
  }
  if (print_event_info->auto_increment_increment != auto_increment_increment ||
      print_event_info->auto_increment_offset != auto_increment_offset)
  {
    my_b_printf(file,"SET @@session.auto_increment_increment=%u, @@session.auto_increment_offset=%u%s\n",
                auto_increment_increment,auto_increment_offset,
                print_event_info->delimiter);
    print_event_info->auto_increment_increment= auto_increment_increment;
    print_event_info->auto_increment_offset=    auto_increment_offset;
  }

  /* TODO: print the catalog when we feature SET CATALOG */

  if (likely(charset_inited) &&
      (unlikely(!print_event_info->charset_inited ||
                memcmp(print_event_info->charset, charset, 6))))
  {
    char *charset_p= charset; // Avoid type-punning warning.
    CHARSET_INFO *cs_info= get_charset(uint2korr(charset_p), MYF(MY_WME));
    if (cs_info)
    {
      /* for mysql client */
      my_b_printf(file, "/*!\\C %s */%s\n",
                  cs_info->csname, print_event_info->delimiter);
    }
    my_b_printf(file,"SET "
                "@@session.character_set_client=%d,"
                "@@session.collation_connection=%d,"
                "@@session.collation_server=%d"
                "%s\n",
                uint2korr(charset_p),
                uint2korr(charset+2),
                uint2korr(charset+4),
                print_event_info->delimiter);
    memcpy(print_event_info->charset, charset, 6);
    print_event_info->charset_inited= 1;
  }
  if (time_zone_len)
  {
    if (memcmp(print_event_info->time_zone_str,
               time_zone_str, time_zone_len+1))
    {
      my_b_printf(file,"SET @@session.time_zone='%s'%s\n",
                  time_zone_str, print_event_info->delimiter);
      memcpy(print_event_info->time_zone_str, time_zone_str, time_zone_len+1);
    }
  }
  if (lc_time_names_number != print_event_info->lc_time_names_number)
  {
    my_b_printf(file, "SET @@session.lc_time_names=%d%s\n",
                lc_time_names_number, print_event_info->delimiter);
    print_event_info->lc_time_names_number= lc_time_names_number;
  }
  if (charset_database_number != print_event_info->charset_database_number)
  {
    if (charset_database_number)
      my_b_printf(file, "SET @@session.collation_database=%d%s\n",
                  charset_database_number, print_event_info->delimiter);
    else
      my_b_printf(file, "SET @@session.collation_database=DEFAULT%s\n",
                  print_event_info->delimiter);
    print_event_info->charset_database_number= charset_database_number;
  }
  if (explicit_defaults_ts != TERNARY_UNSET)
    my_b_printf(file, "SET @@session.explicit_defaults_for_timestamp=%d%s\n",
                explicit_defaults_ts == TERNARY_OFF? 0 : 1,
                print_event_info->delimiter);
}


void Query_log_event::print(FILE* file, PRINT_EVENT_INFO* print_event_info)
{
  IO_CACHE *const head= &print_event_info->head_cache;

  /**
    reduce the size of io cache so that the write function is called
    for every call to my_b_write().
   */
  DBUG_EXECUTE_IF ("simulate_file_write_error",
                   {head->write_pos= head->write_end- 500;});
  print_query_header(head, print_event_info);
  my_b_write(head, (uchar*) query, q_len);
  my_b_printf(head, "\n%s\n", print_event_info->delimiter);
}
#endif /* MYSQL_CLIENT */

#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)

/**
   Associating slave Worker thread to a subset of temporary tables.

   @param thd_arg THD instance pointer
   @param rli     Relay_log_info of the worker
*/
void Query_log_event::attach_temp_tables_worker(THD *thd_arg,
                                                const Relay_log_info* rli)
{
  rli->current_mts_submode->attach_temp_tables(thd_arg, rli, this);
}

/**
   Dissociating slave Worker thread from its thd->temporary_tables
   to possibly update the involved entries of db-to-worker hash
   with new values of temporary_tables.

   @param thd_arg THD instance pointer
   @param rli     relay log info of the worker thread
*/
void Query_log_event::detach_temp_tables_worker(THD *thd_arg,
                                                const Relay_log_info *rli)
{
  rli->current_mts_submode->detach_temp_tables(thd_arg, rli, this);
}

/*
  Query_log_event::do_apply_event()
*/
int Query_log_event::do_apply_event(Relay_log_info const *rli)
{
  return do_apply_event(rli, query, q_len);
}

/*
  is_silent_error

  Return true if the thread has an error which should be
  handled silently
*/
  
static bool is_silent_error(THD* thd)
{
  DBUG_ENTER("is_silent_error");
  Diagnostics_area::Sql_condition_iterator it=
    thd->get_stmt_da()->sql_conditions();
  const Sql_condition *err;
  while ((err= it++))
  {
    DBUG_PRINT("info", ("has condition %d %s", err->mysql_errno(),
                        err->message_text()));
    switch (err->mysql_errno())
    {
    case ER_SLAVE_SILENT_RETRY_TRANSACTION:
    {
      DBUG_RETURN(true);
    }
    default:
      break;
    }
  }
  DBUG_RETURN(false);
}

/**
  @todo
  Compare the values of "affected rows" around here. Something
  like:
  @code
     if ((uint32) affected_in_event != (uint32) affected_on_slave)
     {
     sql_print_error("Slave: did not get the expected number of affected \
     rows running query from master - expected %d, got %d (this numbers \
     should have matched modulo 4294967296).", 0, ...);
     thd->query_error = 1;
     }
  @endcode
  We may also want an option to tell the slave to ignore "affected"
  mismatch. This mismatch could be implemented with a new ER_ code, and
  to ignore it you would use --slave-skip-errors...
*/
int Query_log_event::do_apply_event(Relay_log_info const *rli,
                                      const char *query_arg, size_t q_len_arg)
{
  DBUG_ENTER("Query_log_event::do_apply_event");
  int expected_error,actual_error= 0;
  HA_CREATE_INFO db_options;

  DBUG_PRINT("info", ("query=%s, q_len_arg=%lu",
                      query, static_cast<unsigned long>(q_len_arg)));

  /*
    Colleagues: please never free(thd->catalog) in MySQL. This would
    lead to bugs as here thd->catalog is a part of an alloced block,
    not an entire alloced block (see
    Query_log_event::do_apply_event()). Same for thd->db().str.  Thank
    you.
  */

  if (catalog_len)
  {
    LEX_CSTRING catalog_lex_cstr= { catalog, catalog_len};
    thd->set_catalog(catalog_lex_cstr);
  }
  else
    thd->set_catalog(EMPTY_CSTR);

  size_t valid_len;
  bool len_error;
  bool is_invalid_db_name= validate_string(system_charset_info, db, db_len,
                                           &valid_len, &len_error);

  DBUG_PRINT("debug",("is_invalid_db_name= %s, valid_len=%zu, len_error=%s",
                      is_invalid_db_name ? "true" : "false",
                      valid_len,
                      len_error ? "true" : "false"));

  if (is_invalid_db_name || len_error)
  {
    rli->report(ERROR_LEVEL, ER_SLAVE_FATAL_ERROR,
                ER_THD(thd, ER_SLAVE_FATAL_ERROR),
                "Invalid database name in Query event.");
    thd->is_slave_error= true;
    goto end;
  }

  set_thd_db(thd, db, db_len);

  /*
    Setting the character set and collation of the current database thd->db.
   */
  load_db_opt_by_name(thd, thd->db().str, &db_options);
  if (db_options.default_table_charset)
    thd->db_charset= db_options.default_table_charset;
  thd->variables.auto_increment_increment= auto_increment_increment;
  thd->variables.auto_increment_offset=    auto_increment_offset;
  if (explicit_defaults_ts != TERNARY_UNSET)
    thd->variables.explicit_defaults_for_timestamp=
      explicit_defaults_ts == TERNARY_OFF? 0 : 1;

  /*
    todo: such cleanup should not be specific to Query event and therefore
          is preferable at a common with other event pre-execution point
  */
  clear_all_errors(thd, const_cast<Relay_log_info*>(rli));
  if (strcmp("COMMIT", query) == 0 && rli->tables_to_lock != NULL)
  {
    /*
      Cleaning-up the last statement context:
      the terminal event of the current statement flagged with
      STMT_END_F got filtered out in ndb circular replication.
    */
    int error;
    char llbuff[22];
    if ((error= rows_event_stmt_cleanup(const_cast<Relay_log_info*>(rli), thd)))
    {
      const_cast<Relay_log_info*>(rli)->report(ERROR_LEVEL, error,
                  "Error in cleaning up after an event preceding the commit; "
                  "the group log file/position: %s %s",
                  const_cast<Relay_log_info*>(rli)->get_group_master_log_name(),
                  llstr(const_cast<Relay_log_info*>(rli)->get_group_master_log_pos(),
                        llbuff));
    }
    /*
      Executing a part of rli->stmt_done() logics that does not deal
      with group position change. The part is redundant now but is 
      future-change-proof addon, e.g if COMMIT handling will start checking
      invariants like IN_STMT flag must be off at committing the transaction.
    */
    const_cast<Relay_log_info*>(rli)->inc_event_relay_log_pos();
    const_cast<Relay_log_info*>(rli)->clear_flag(Relay_log_info::IN_STMT);
  }
  else
  {
    const_cast<Relay_log_info*>(rli)->slave_close_thread_tables(thd);
  }

  {
    thd->set_time(&(common_header->when));
    thd->set_query(query_arg, q_len_arg);
    thd->set_query_id(next_query_id());
    thd->variables.pseudo_thread_id= thread_id;		// for temp tables
    attach_temp_tables_worker(thd, rli);
    DBUG_PRINT("query",("%s", thd->query().str));

    if (ignored_error_code((expected_error= error_code)) ||
	!unexpected_error_code(expected_error))
    {
      if (flags2_inited)
        /*
          all bits of thd->variables.option_bits which are 1 in OPTIONS_WRITTEN_TO_BIN_LOG
          must take their value from flags2.
        */
        thd->variables.option_bits= flags2|(thd->variables.option_bits & ~OPTIONS_WRITTEN_TO_BIN_LOG);
      /*
        else, we are in a 3.23/4.0 binlog; we previously received a
        Rotate_log_event which reset thd->variables.option_bits and sql_mode etc, so
        nothing to do.
      */
      /*
        We do not replicate MODE_NO_DIR_IN_CREATE. That is, if the master is a
        slave which runs with SQL_MODE=MODE_NO_DIR_IN_CREATE, this should not
        force us to ignore the dir too. Imagine you are a ring of machines, and
        one has a disk problem so that you temporarily need
        MODE_NO_DIR_IN_CREATE on this machine; you don't want it to propagate
        elsewhere (you don't want all slaves to start ignoring the dirs).
      */
      if (sql_mode_inited)
        thd->variables.sql_mode=
          (sql_mode_t) ((thd->variables.sql_mode & MODE_NO_DIR_IN_CREATE) |
                       (sql_mode & ~(ulonglong) MODE_NO_DIR_IN_CREATE));
      if (charset_inited)
      {
        if (rli->cached_charset_compare(charset))
        {
          char *charset_p= charset; // Avoid type-punning warning.
          /* Verify that we support the charsets found in the event. */
          if (!(thd->variables.character_set_client=
                get_charset(uint2korr(charset_p), MYF(MY_WME))) ||
              !(thd->variables.collation_connection=
                get_charset(uint2korr(charset+2), MYF(MY_WME))) ||
              !(thd->variables.collation_server=
                get_charset(uint2korr(charset+4), MYF(MY_WME))))
          {
            /*
              We updated the thd->variables with nonsensical values (0). Let's
              set them to something safe (i.e. which avoids crash), and we'll
              stop with EE_UNKNOWN_CHARSET in compare_errors (unless set to
              ignore this error).
            */
            set_slave_thread_default_charset(thd, rli);
            goto compare_errors;
          }
          thd->update_charset(); // for the charset change to take effect
          /*
            We cannot ask for parsing a statement using a character set
            without state_maps (parser internal data).
          */
          if (!thd->variables.character_set_client->state_maps)
          {
            rli->report(ERROR_LEVEL, ER_SLAVE_FATAL_ERROR,
                        ER_THD(thd, ER_SLAVE_FATAL_ERROR),
                        "character_set cannot be parsed");
            thd->is_slave_error= true;
            goto end;
          }
          /*
            Reset thd->query_string.cs to the newly set value.
            Note, there is a small flaw here. For a very short time frame
            if the new charset is different from the old charset and
            if another thread executes "SHOW PROCESSLIST" after
            the above thd->set_query() and before this thd->set_query(),
            and if the current query has some non-ASCII characters,
            the another thread may see some '?' marks in the PROCESSLIST
            result. This should be acceptable now. This is a reminder
            to fix this if any refactoring happens here sometime.
          */
          thd->set_query(query_arg, q_len_arg);
        }
      }
      if (time_zone_len)
      {
        String tmp(time_zone_str, time_zone_len, &my_charset_bin);
        if (!(thd->variables.time_zone= my_tz_find(thd, &tmp)))
        {
          my_error(ER_UNKNOWN_TIME_ZONE, MYF(0), tmp.c_ptr());
          thd->variables.time_zone= global_system_variables.time_zone;
          goto compare_errors;
        }
      }
      if (lc_time_names_number)
      {
        if (!(thd->variables.lc_time_names=
              my_locale_by_number(lc_time_names_number)))
        {
          my_printf_error(ER_UNKNOWN_ERROR,
                      "Unknown locale: '%d'", MYF(0), lc_time_names_number);
          thd->variables.lc_time_names= &my_locale_en_US;
          goto compare_errors;
        }
      }
      else
        thd->variables.lc_time_names= &my_locale_en_US;
      if (charset_database_number)
      {
        CHARSET_INFO *cs;
        if (!(cs= get_charset(charset_database_number, MYF(0))))
        {
          char buf[20];
          int10_to_str((int) charset_database_number, buf, -10);
          my_error(ER_UNKNOWN_COLLATION, MYF(0), buf);
          goto compare_errors;
        }
        thd->variables.collation_database= cs;
      }
      else
        thd->variables.collation_database= thd->db_charset;

      thd->table_map_for_update= (table_map)table_map_for_update;

      LEX_STRING user_lex= LEX_STRING();
      LEX_STRING host_lex= LEX_STRING();
      if (user)
      {
        user_lex.str= const_cast<char*>(user);
        user_lex.length= strlen(user);
      }
      if (host)
      {
        host_lex.str= const_cast<char*>(host);
        host_lex.length= strlen(host);
      }
      thd->set_invoker(&user_lex, &host_lex);
      /*
        Flag if we need to rollback the statement transaction on
        slave if it by chance succeeds.
        If we expected a non-zero error code and get nothing and,
        it is a concurrency issue or ignorable issue, effects
        of the statement should be rolled back.
      */
      if (expected_error &&
          (ignored_error_code(expected_error) ||
           concurrency_error_code(expected_error)))
      {
        thd->variables.option_bits|= OPTION_MASTER_SQL_ERROR;
      }
      /* Execute the query (note that we bypass dispatch_command()) */
      Parser_state parser_state;
      if (!parser_state.init(thd, thd->query().str, thd->query().length))
      {
        DBUG_ASSERT(thd->m_digest == NULL);
        thd->m_digest= & thd->m_digest_state;
        DBUG_ASSERT(thd->m_statement_psi == NULL);
        thd->m_statement_psi= MYSQL_START_STATEMENT(&thd->m_statement_state,
                                                    stmt_info_rpl.m_key,
                                                    thd->db().str,
                                                    thd->db().length,
                                                    thd->charset(), NULL);
        THD_STAGE_INFO(thd, stage_starting);

        if (thd->m_digest != NULL)
          thd->m_digest->reset(thd->m_token_array, max_digest_length);

        mysql_parse(thd, &parser_state);

        /*
          Transaction isolation level of pure row based replicated transactions
          can be optimized to ISO_READ_COMMITTED by the applier when applying
          the Gtid_log_event.

          If we are applying a statement other than transaction control ones
          after having optimized the transactions isolation level, we must warn
          about the non-standard situation we have found.
        */
        if (is_sbr_logging_format() &&
            thd->variables.tx_isolation > ISO_READ_COMMITTED &&
            thd->tx_isolation == ISO_READ_COMMITTED)
        {
          String message;
          message.append("The isolation level for the current transaction "
                         "was changed to READ_COMMITTED based on the "
                         "assumption that it had only row events and was "
                         "not mixed with statements. "
                         "However, an unexpected statement was found in "
                         "the middle of the transaction."
                         "Query: '");
          message.append(thd->query().str);
          message.append("'");
          rli->report(ERROR_LEVEL, ER_SLAVE_FATAL_ERROR,
                      ER_THD(thd, ER_SLAVE_FATAL_ERROR),
                      message.c_ptr());
          thd->is_slave_error= true;
          goto end;
        }

        /* Finalize server status flags after executing a statement. */
        thd->update_server_status();
        log_slow_statement(thd);
      }

      thd->variables.option_bits&= ~OPTION_MASTER_SQL_ERROR;

      /*
        Resetting the enable_slow_log thd variable.

        We need to reset it back to the opt_log_slow_slave_statements
        value after the statement execution (and slow logging
        is done). It might have changed if the statement was an
        admin statement (in which case, down in mysql_parse execution
        thd->enable_slow_log is set to the value of
        opt_log_slow_admin_statements).
      */
      thd->enable_slow_log= opt_log_slow_slave_statements;
    }
    else
    {
      /*
        The query got a really bad error on the master (thread killed etc),
        which could be inconsistent. Parse it to test the table names: if the
        replicate-*-do|ignore-table rules say "this query must be ignored" then
        we exit gracefully; otherwise we warn about the bad error and tell DBA
        to check/fix it.
      */
      if (mysql_test_parse_for_slave(thd))
        clear_all_errors(thd, const_cast<Relay_log_info*>(rli)); /* Can ignore query */
      else
      {
        rli->report(ERROR_LEVEL, ER_ERROR_ON_MASTER, ER(ER_ERROR_ON_MASTER),
                    expected_error, thd->query().str);
        thd->is_slave_error= 1;
      }
      goto end;
    }

    /* If the query was not ignored, it is printed to the general log */
    if (!thd->is_error() ||
        thd->get_stmt_da()->mysql_errno() != ER_SLAVE_IGNORED_TABLE)
    {
      /* log the rewritten query if the query was rewritten 
         and the option to log raw was not set.
        
         There is an assumption here. We assume that query log
         events can never have multi-statement queries, thus the
         parsed statement is the same as the raw one.
       */
      if (opt_general_log_raw || thd->rewritten_query.length() == 0)
        query_logger.general_log_write(thd, COM_QUERY, thd->query().str,
                                       thd->query().length);
      else
        query_logger.general_log_write(thd, COM_QUERY,
                                       thd->rewritten_query.c_ptr_safe(),
                                       thd->rewritten_query.length());
    }

compare_errors:
    /* Parser errors shall be ignored when (GTID) skipping statements */
    if (thd->is_error() &&
        thd->get_stmt_da()->mysql_errno() == ER_PARSE_ERROR &&
        gtid_pre_statement_checks(thd) == GTID_STATEMENT_SKIP)
    {
      thd->get_stmt_da()->reset_diagnostics_area();
    }
    /*
      In the slave thread, we may sometimes execute some DROP / * 40005
      TEMPORARY * / TABLE that come from parts of binlogs (likely if we
      use RESET SLAVE or CHANGE MASTER TO), while the temporary table
      has already been dropped. To ignore such irrelevant "table does
      not exist errors", we silently clear the error if TEMPORARY was used.
    */
    if (thd->lex->sql_command == SQLCOM_DROP_TABLE &&
        thd->lex->drop_temporary &&
        thd->is_error() &&
        thd->get_stmt_da()->mysql_errno() == ER_BAD_TABLE_ERROR &&
        !expected_error)
      thd->get_stmt_da()->reset_diagnostics_area();
    /*
      If we expected a non-zero error code, and we don't get the same error
      code, and it should be ignored or is related to a concurrency issue.
    */
    actual_error= thd->is_error() ? thd->get_stmt_da()->mysql_errno() : 0;
    DBUG_PRINT("info",("expected_error: %d  sql_errno: %d",
                       expected_error, actual_error));

    /*
      If a statement with expected error is received on slave and if the
      statement is not filtered on the slave, only then compare the expected
      error with the actual error that happened on slave.
    */
    if ((expected_error && rpl_filter->db_ok(thd->db().str) &&
         expected_error != actual_error &&
         !concurrency_error_code(expected_error)) &&
        !ignored_error_code(actual_error) &&
        !ignored_error_code(expected_error))
    {
      if (!ignored_error_code(ER_INCONSISTENT_ERROR))
      {
        rli->report(ERROR_LEVEL, ER_INCONSISTENT_ERROR,
                    ER(ER_INCONSISTENT_ERROR),
                    ER_THD(thd, expected_error), expected_error,
                    (actual_error ?
                     thd->get_stmt_da()->message_text() :
                     "no error"),
                    actual_error, print_slave_db_safe(db), query_arg);
        thd->is_slave_error= 1;
      }
      else
      {
        rli->report(INFORMATION_LEVEL, actual_error,
                    "The actual error and expected error on slave are"
                    " different that will result in ER_INCONSISTENT_ERROR but"
                    " that is passed as an argument to slave_skip_errors so no"
                    " error is thrown. "
                    "The expected error was %s with, Error_code: %d. "
                    "The actual error is %s with ",
                    ER(expected_error), expected_error,
                    thd->get_stmt_da()->message_text());
        clear_all_errors(thd, const_cast<Relay_log_info*>(rli));
      }
    }
    /*
      If we get the same error code as expected and it is not a concurrency
      issue, or should be ignored.
    */
    else if ((expected_error == actual_error &&
              !concurrency_error_code(expected_error)) ||
             ignored_error_code(actual_error))
    {
      DBUG_PRINT("info",("error ignored"));
      if (actual_error && ignored_error_code(actual_error))
      {
        if (actual_error == ER_SLAVE_IGNORED_TABLE)
        {
          if (!slave_ignored_err_throttle.log())
            rli->report(INFORMATION_LEVEL, actual_error,
                        "Could not execute %s event. Detailed error: %s;"
                        " Error log throttle is enabled. This error will not be"
                        " displayed for next %lu secs. It will be suppressed",
                        get_type_str(), thd->get_stmt_da()->message_text(),
                        (window_size / 1000000));
        }
        else
          rli->report(INFORMATION_LEVEL, actual_error,
                      "Could not execute %s event. Detailed error: %s;",
                      get_type_str(), thd->get_stmt_da()->message_text());
      }
      clear_all_errors(thd, const_cast<Relay_log_info*>(rli));
      thd->killed= THD::NOT_KILLED;
    }
    /*
      Other cases: mostly we expected no error and get one.
    */
    else if (thd->is_slave_error || thd->is_fatal_error)
    {
      if (!is_silent_error(thd))
      {
        rli->report(ERROR_LEVEL, actual_error,
                    "Error '%s' on query. Default database: '%s'. Query: '%s'",
                    (actual_error ?
                     thd->get_stmt_da()->message_text() :
                     "unexpected success or fatal error"),
                    print_slave_db_safe(thd->db().str), query_arg);
      }
      thd->is_slave_error= 1;
    }

    /*
      TODO: compare the values of "affected rows" around here. Something
      like:
      if ((uint32) affected_in_event != (uint32) affected_on_slave)
      {
      sql_print_error("Slave: did not get the expected number of affected \
      rows running query from master - expected %d, got %d (this numbers \
      should have matched modulo 4294967296).", 0, ...);
      thd->is_slave_error = 1;
      }
      We may also want an option to tell the slave to ignore "affected"
      mismatch. This mismatch could be implemented with a new ER_ code, and
      to ignore it you would use --slave-skip-errors...

      To do the comparison we need to know the value of "affected" which the
      above mysql_parse() computed. And we need to know the value of
      "affected" in the master's binlog. Both will be implemented later. The
      important thing is that we now have the format ready to log the values
      of "affected" in the binlog. So we can release 5.0.0 before effectively
      logging "affected" and effectively comparing it.
    */
  } /* End of if (db_ok(... */

  {
    /**
      The following failure injecion works in cooperation with tests
      setting @@global.debug= 'd,stop_slave_middle_group'.
      The sql thread receives the killed status and will proceed
      to shutdown trying to finish incomplete events group.
    */

    // TODO: address the middle-group killing in MTS case

    DBUG_EXECUTE_IF("stop_slave_middle_group",
                    if (strcmp("COMMIT", query) != 0 &&
                        strcmp("BEGIN", query) != 0)
                    {
                      if (thd->get_transaction()->cannot_safely_rollback(
                          Transaction_ctx::SESSION))
                        const_cast<Relay_log_info*>(rli)->abort_slave= 1;
                    };);
  }

end:

  if (thd->temporary_tables)
    detach_temp_tables_worker(thd, rli);
  /*
    Probably we have set thd->query, thd->db, thd->catalog to point to places
    in the data_buf of this event. Now the event is going to be deleted
    probably, so data_buf will be freed, so the thd->... listed above will be
    pointers to freed memory.
    So we must set them to 0, so that those bad pointers values are not later
    used. Note that "cleanup" queries like automatic DROP TEMPORARY TABLE
    don't suffer from these assignments to 0 as DROP TEMPORARY
    TABLE uses the db.table syntax.
  */
  thd->set_catalog(NULL_CSTR);
  thd->set_db(NULL_CSTR);                 /* will free the current database */
  thd->reset_query();
  thd->lex->sql_command= SQLCOM_END;
  DBUG_PRINT("info", ("end: query= 0"));

  /* Mark the statement completed. */
  MYSQL_END_STATEMENT(thd->m_statement_psi, thd->get_stmt_da());
  thd->m_statement_psi= NULL;
  thd->m_digest= NULL;

  /*
    As a disk space optimization, future masters will not log an event for
    LAST_INSERT_ID() if that function returned 0 (and thus they will be able
    to replace the THD::stmt_depends_on_first_successful_insert_id_in_prev_stmt
    variable by (THD->first_successful_insert_id_in_prev_stmt > 0) ; with the
    resetting below we are ready to support that.
  */
  thd->first_successful_insert_id_in_prev_stmt_for_binlog= 0;
  thd->first_successful_insert_id_in_prev_stmt= 0;
  thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt= 0;
  free_root(thd->mem_root,MYF(MY_KEEP_PREALLOC));
  DBUG_RETURN(thd->is_slave_error);
}

int Query_log_event::do_update_pos(Relay_log_info *rli)
{
  int ret= Log_event::do_update_pos(rli);

  DBUG_EXECUTE_IF("crash_after_commit_and_update_pos",
       if (!strcmp("COMMIT", query))
       {
         sql_print_information("Crashing crash_after_commit_and_update_pos.");
         rli->flush_info(true);
         ha_flush_logs(0); 
         DBUG_SUICIDE();
       }
  );
  
  return ret;
}


Log_event::enum_skip_reason
Query_log_event::do_shall_skip(Relay_log_info *rli)
{
  DBUG_ENTER("Query_log_event::do_shall_skip");
  DBUG_PRINT("debug", ("query: %s; q_len: %d", query, static_cast<int>(q_len)));
  DBUG_ASSERT(query && q_len > 0);

  if (rli->slave_skip_counter > 0)
  {
    if (strcmp("BEGIN", query) == 0)
    {
      thd->variables.option_bits|= OPTION_BEGIN;
      DBUG_RETURN(Log_event::continue_group(rli));
    }

    if (strcmp("COMMIT", query) == 0 || strcmp("ROLLBACK", query) == 0)
    {
      thd->variables.option_bits&= ~OPTION_BEGIN;
      DBUG_RETURN(Log_event::EVENT_SKIP_COUNT);
    }
  }
  Log_event::enum_skip_reason ret= Log_event::do_shall_skip(rli);
  DBUG_RETURN(ret);
}

#endif

/**
   Return the query string pointer (and its size) from a Query log event
   using only the event buffer (we don't instantiate a Query_log_event
   object for this).

   @param buf               Pointer to the event buffer.
   @param length            The size of the event buffer.
   @param description_event The description event of the master which logged
                            the event.
   @param[out] query        The pointer to receive the query pointer.

   @return                  The size of the query.
*/
size_t Query_log_event::get_query(const char *buf, size_t length,
                                  const Format_description_log_event *fd_event,
                                  char** query)
{
  //DBUG_ASSERT((Log_event_type)buf[EVENT_TYPE_OFFSET] ==
  //            binary_log::QUERY_EVENT);

  char db_len;                                  /* size of db name */
  uint status_vars_len= 0;                      /* size of status_vars */
  size_t qlen;                                  /* size of the query */
  int checksum_size= 0;                         /* size of trailing checksum */
  const char *end_of_query;

  uint common_header_len= fd_event->common_header_len;
  uint query_header_len= fd_event->post_header_len[binary_log::QUERY_EVENT-1];

  /* Error if the event content is too small */
  if (length < (common_header_len + query_header_len))
    goto err;

  /* Skip the header */
  buf+= common_header_len;

  /* Check if there are status variables in the event */
  if ((query_header_len - QUERY_HEADER_MINIMAL_LEN) > 0)
  {
    status_vars_len= uint2korr(buf + Q_STATUS_VARS_LEN_OFFSET);
  }

  /* Check if the event has trailing checksum */
  if (fd_event->common_footer->checksum_alg != binary_log::BINLOG_CHECKSUM_ALG_OFF)
    checksum_size= 4;

  db_len= (uchar)buf[Q_DB_LEN_OFFSET];

  /* Error if the event content is too small */
  if (length < (common_header_len + query_header_len +
                db_len + 1 + status_vars_len + checksum_size))
    goto err;

  *query= (char *)buf + query_header_len + db_len + 1 + status_vars_len;

  /* Calculate the query length */
  end_of_query= buf + (length - common_header_len) - /* we skipped the header */
                checksum_size;
  qlen= end_of_query - *query;
  return qlen;

err:
  *query= NULL;
  return 0;
}




/*
  Start_log_event_v3::pack_info()
*/

#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
int Start_log_event_v3::pack_info(Protocol *protocol)
{
  char buf[12 + ST_SERVER_VER_LEN + 14 + 22], *pos;
  pos= my_stpcpy(buf, "Server ver: ");
  pos= my_stpcpy(pos, server_version);
  pos= my_stpcpy(pos, ", Binlog ver: ");
  pos= int10_to_str(binlog_version, pos, 10);
  protocol->store(buf, (uint) (pos-buf), &my_charset_bin);
  return 0;
}
#endif


/*
  Start_log_event_v3::print()
*/

#ifdef MYSQL_CLIENT
void Start_log_event_v3::print(FILE* file, PRINT_EVENT_INFO* print_event_info)
{
  DBUG_ENTER("Start_log_event_v3::print");

  IO_CACHE *const head= &print_event_info->head_cache;

  if (!print_event_info->short_form)
  {
    print_header(head, print_event_info, FALSE);
    my_b_printf(head, "\tStart: binlog v %d, server v %s created ",
                binlog_version, server_version);
    print_timestamp(head, NULL);
    if (created)
      my_b_printf(head," at startup");
    my_b_printf(head, "\n");
    if (common_header->flags & LOG_EVENT_BINLOG_IN_USE_F)
      my_b_printf(head, "# Warning: this binlog is either in use or was not "
                  "closed properly.\n");
  }

  if (is_relay_log_event())
  {
    my_b_printf(head, "# This Format_description_event appears in a relay log "
                "and was generated by the slave thread.\n");
    DBUG_VOID_RETURN;
  }

  if (!is_artificial_event() && created)
  {
#ifdef WHEN_WE_HAVE_THE_RESET_CONNECTION_SQL_COMMAND
    /*
      This is for mysqlbinlog: like in replication, we want to delete the stale
      tmp files left by an unclean shutdown of mysqld (temporary tables)
      and rollback unfinished transaction.
      Probably this can be done with RESET CONNECTION (syntax to be defined).
    */
    my_b_printf(head,"RESET CONNECTION%s\n", print_event_info->delimiter);
#else
    my_b_printf(head,"ROLLBACK%s\n", print_event_info->delimiter);
#endif
  }
  if (temp_buf &&
      print_event_info->base64_output_mode != BASE64_OUTPUT_NEVER &&
      !print_event_info->short_form)
  {
    if (print_event_info->base64_output_mode != BASE64_OUTPUT_DECODE_ROWS)
      my_b_printf(head, "BINLOG '\n");
    print_base64(head, print_event_info, FALSE);
    print_event_info->printed_fd_event= TRUE;

    /*
      If --skip-gtids is given, the server when it replays the output
      should generate a new GTID if gtid_mode=ON.  However, when the
      server reads the base64-encoded Format_description_log_event, it
      will cleverly detect that this is a binlog to be replayed, and
      act a little bit like the replication thread, in the following
      sense: if the thread does not see any 'SET GTID_NEXT' statement,
      it will assume the binlog was created by an old server and try
      to preserve transactions as anonymous.  This is the opposite of
      what we want when passing the --skip-gtids flag, so therefore we
      output the following statement.

      The behavior where the client preserves transactions following a
      Format_description_log_event as anonymous was introduced in
      5.6.16.
    */
    if (print_event_info->skip_gtids)
      my_b_printf(head, "/*!50616 SET @@SESSION.GTID_NEXT='AUTOMATIC'*/%s\n",
                  print_event_info->delimiter);
  }
  DBUG_VOID_RETURN;
}
#endif /* MYSQL_CLIENT */

/*
  Start_log_event_v3::Start_log_event_v3()
*/

Start_log_event_v3::Start_log_event_v3(const char* buf, uint event_len,
                                       const Format_description_event
                                       *description_event)
: binary_log::Start_event_v3(buf, event_len, description_event),
  Log_event(header(), footer())
{
  is_valid_param= server_version[0] != 0;
  if (event_len < (uint)description_event->common_header_len +
      ST_COMMON_HEADER_LEN_OFFSET)
  {
    //erver_version[0]= 0;
    return;
  }
  buf+= description_event->common_header_len;
  binlog_version= uint2korr(buf+ST_BINLOG_VER_OFFSET);
  memcpy(server_version, buf+ST_SERVER_VER_OFFSET,ST_SERVER_VER_LEN);
  // prevent overrun if log is corrupted on disk
  server_version[ST_SERVER_VER_LEN-1]= 0;
  created= uint4korr(buf+ST_CREATED_OFFSET);
  dont_set_created= 1;
}



#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)

/**
  Start_log_event_v3::do_apply_event() .
  The master started

    IMPLEMENTATION
    - To handle the case where the master died without having time to write
    DROP TEMPORARY TABLE, DO RELEASE_LOCK (prepared statements' deletion is
    TODO), we clean up all temporary tables that we got, if we are sure we
    can (see below).

  @todo
    - Remove all active user locks.
    Guilhem 2003-06: this is true but not urgent: the worst it can cause is
    the use of a bit of memory for a user lock which will not be used
    anymore. If the user lock is later used, the old one will be released. In
    other words, no deadlock problem.
*/

int Start_log_event_v3::do_apply_event(Relay_log_info const *rli)
{
  DBUG_ENTER("Start_log_event_v3::do_apply_event");
  int error= 0;
  switch (binlog_version)
  {
  case 3:
  case 4:
    /*
      This can either be 4.x (then a Start_log_event_v3 is only at master
      startup so we are sure the master has restarted and cleared his temp
      tables; the event always has 'created'>0) or 5.0 (then we have to test
      'created').
    */
    if (created)
    {
      error= close_temporary_tables(thd);
      cleanup_load_tmpdir();
    }
    else
    {
      /*
        Set all temporary tables thread references to the current thread
        as they may point to the "old" SQL slave thread in case of its
        restart.
      */
      TABLE *table;
      for (table= thd->temporary_tables; table; table= table->next)
        table->in_use= thd;
    }
    break;

    /*
       Now the older formats; in that case load_tmpdir is cleaned up by the I/O
       thread.
    */
  case 1:
    if (strncmp(rli->get_rli_description_event()->server_version,
                "3.23.57",7) >= 0 && created)
    {
      /*
        Can distinguish, based on the value of 'created': this event was
        generated at master startup.
      */
      error= close_temporary_tables(thd);
    }
    /*
      Otherwise, can't distinguish a Start_log_event generated at
      master startup and one generated by master FLUSH LOGS, so cannot
      be sure temp tables have to be dropped. So do nothing.
    */
    break;
  default:
    /*
      This case is not expected. It can be either an event corruption or an
      unsupported binary log version.
    */
    rli->report(ERROR_LEVEL, ER_SLAVE_FATAL_ERROR,
                ER_THD(thd, ER_SLAVE_FATAL_ERROR),
                "Binlog version not supported");
    DBUG_RETURN(1);
  }
  DBUG_RETURN(error);
}
#endif /* defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT) */

/***************************************************************************
       Format_description_log_event methods
****************************************************************************/

/**
  Format_description_log_event 1st ctor.

    Ctor. Can be used to create the event to write to the binary log (when the
    server starts or when FLUSH LOGS), or to create artificial events to parse
    binlogs from MySQL 3.23 or 4.x.
    When in a client, only the 2nd use is possible.

  @param binlog_version         the binlog version for which we want to build
                                an event. Can be 1 (=MySQL 3.23), 3 (=4.0.x
                                x>=2 and 4.1) or 4 (MySQL 5.0). Note that the
                                old 4.0 (binlog version 2) is not supported;
                                it should not be used for replication with
                                5.0.
  @param server_ver             a string containing the server version.
*/

Format_description_log_event::Format_description_log_event(uint8_t binlog_ver, const char* server_ver)
: binary_log::Start_event_v3(binary_log::FORMAT_DESCRIPTION_EVENT),
  Format_description_event(binlog_ver,  (binlog_ver <= 3 || server_ver != 0) ?
                           server_ver : server_version)
{
  is_valid_param= header_is_valid() && version_is_valid();
  common_header->type_code= binary_log::FORMAT_DESCRIPTION_EVENT;
  /*
   We here have the possibility to simulate a master before we changed
   the table map id to be stored in 6 bytes: when it was stored in 4
   bytes (=> post_header_len was 6). This is used to test backward
   compatibility.
   This code can be removed after a few months (today is Dec 21st 2005),
   when we know that the 4-byte masters are not deployed anymore (check
   with Tomas Ulin first!), and the accompanying test (rpl_row_4_bytes)
   too.
  */
                  post_header_len[binary_log::TABLE_MAP_EVENT-1]=
                  post_header_len[binary_log::WRITE_ROWS_EVENT_V1-1]=
                  post_header_len[binary_log::UPDATE_ROWS_EVENT_V1-1]=
                  post_header_len[binary_log::DELETE_ROWS_EVENT_V1-1]= 6;
}


/**
  The problem with this constructor is that the fixed header may have a
  length different from this version, but we don't know this length as we
  have not read the Format_description_log_event which says it, yet. This
  length is in the post-header of the event, but we don't know where the
  post-header starts.

  So this type of event HAS to:
  - either have the header's length at the beginning (in the header, at a
  fixed position which will never be changed), not in the post-header. That
  would make the header be "shifted" compared to other events.
  - or have a header of size LOG_EVENT_MINIMAL_HEADER_LEN (19), in all future
  versions, so that we know for sure.

  I (Guilhem) chose the 2nd solution. Rotate has the same constraint (because
  it is sent before Format_description_log_event).
*/

Format_description_log_event::
Format_description_log_event(const char* buf, uint event_len,
                             const Format_description_event
                             *description_event)
  : binary_log::Start_event_v3(buf, event_len, description_event),
    Format_description_event(buf, event_len, description_event),
    Start_log_event_v3(buf, event_len, description_event)
{
  is_valid_param= header_is_valid() && version_is_valid();
  common_header->type_code= binary_log::FORMAT_DESCRIPTION_EVENT;

  /*
   We here have the possibility to simulate a master of before we changed
   the table map id to be stored in 6 bytes: when it was stored in 4
   bytes (=> post_header_len was 6). This is used to test backward
   compatibility.
 */

}



#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
int Format_description_log_event::do_apply_event(Relay_log_info const *rli)
{
  int ret= 0;
  DBUG_ENTER("Format_description_log_event::do_apply_event");

  /*
    As a transaction NEVER spans on 2 or more binlogs:
    if we have an active transaction at this point, the master died
    while writing the transaction to the binary log, i.e. while
    flushing the binlog cache to the binlog. XA guarantees that master has
    rolled back. So we roll back.
    Note: this event could be sent by the master to inform us of the
    format of its binlog; in other words maybe it is not at its
    original place when it comes to us; we'll know this by checking
    log_pos ("artificial" events have log_pos == 0).
  */
  if (!thd->rli_fake && !is_artificial_event() && created &&
      thd->get_transaction()->is_active(Transaction_ctx::SESSION))
  {
    /* This is not an error (XA is safe), just an information */
    rli->report(INFORMATION_LEVEL, 0,
                "Rolling back unfinished transaction (no COMMIT "
                "or ROLLBACK in relay log). A probable cause is that "
                "the master died while writing the transaction to "
                "its binary log, thus rolled back too."); 
    const_cast<Relay_log_info*>(rli)->cleanup_context(thd, 1);
  }

  /*
    If this event comes from ourselves, there is no cleaning task to
    perform, we don't call Start_log_event_v3::do_apply_event()
    (this was just to update the log's description event).
  */
  if (server_id != (uint32) ::server_id)
  {
    /*
      If the event was not requested by the slave i.e. the master sent
      it while the slave asked for a position >4, the event will make
      rli->group_master_log_pos advance. Say that the slave asked for
      position 1000, and the Format_desc event's end is 96. Then in
      the beginning of replication rli->group_master_log_pos will be
      0, then 96, then jump to first really asked event (which is
      >96). So this is ok.
    */
    ret= Start_log_event_v3::do_apply_event(rli);
  }

  if (!ret)
  {
    /* Save the information describing this binlog */
    const_cast<Relay_log_info *>(rli)->set_rli_description_event(this);
  }

  DBUG_RETURN(ret);
}

int Format_description_log_event::do_update_pos(Relay_log_info *rli)
{
  if (server_id == (uint32) ::server_id)
  {
    /*
      We only increase the relay log position if we are skipping
      events and do not touch any group_* variables, nor flush the
      relay log info.  If there is a crash, we will have to re-skip
      the events again, but that is a minor issue.

      If we do not skip stepping the group log position (and the
      server id was changed when restarting the server), it might well
      be that we start executing at a position that is invalid, e.g.,
      at a Rows_log_event or a Query_log_event preceeded by a
      Intvar_log_event instead of starting at a Table_map_log_event or
      the Intvar_log_event respectively.
     */
    rli->inc_event_relay_log_pos();
    return 0;
  }
  else
  {
    return Log_event::do_update_pos(rli);
  }
}

Log_event::enum_skip_reason
Format_description_log_event::do_shall_skip(Relay_log_info *rli)
{
  return Log_event::EVENT_SKIP_NOT;
}

#endif



  /**************************************************************************
        Load_log_event methods
   General note about Load_log_event: the binlogging of LOAD DATA INFILE is
   going to be changed in 5.0 (or maybe in 5.1; not decided yet).
   However, the 5.0 slave could still have to read such events (from a 4.x
   master), convert them (which just means maybe expand the header, when 5.0
   servers have a UID in events) (remember that whatever is after the header
   will be like in 4.x, as this event's format is not modified in 5.0 as we
   will use new types of events to log the new LOAD DATA INFILE features).
   To be able to read/convert, we just need to not assume that the common
   header is of length LOG_EVENT_HEADER_LEN (we must use the description
   event).
   Note that I (Guilhem) manually tested replication of a big LOAD DATA INFILE
   between 3.23 and 5.0, and between 4.0 and 5.0, and it works fine (and the
   positions displayed in SHOW SLAVE STATUS then are fine too).
  **************************************************************************/

#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
uint Load_log_event::get_query_buffer_length()
{
  return
    //the DB name may double if we escape the quote character
    5 + 2*db_len + 3 +
    18 + fname_len*4 + 2 +                    // "LOAD DATA INFILE 'file''"
    11 +                                    // "CONCURRENT "
    7 +					    // LOCAL
    9 +                                     // " REPLACE or IGNORE "
    13 + table_name_len*2 +                 // "INTO TABLE `table`"
    21 + sql_ex.data_info.field_term_len*4 + 2 +
                                            // " FIELDS TERMINATED BY 'str'"
    23 + sql_ex.data_info.enclosed_len*4 + 2 +
                                            // " OPTIONALLY ENCLOSED BY 'str'"
    12 + sql_ex.data_info.escaped_len*4 + 2 +         // " ESCAPED BY 'str'"
    21 + sql_ex.data_info.line_term_len*4 + 2 +
                                            // " LINES TERMINATED BY 'str'"
    19 + sql_ex.data_info.line_start_len*4 + 2 +
                                            // " LINES STARTING BY 'str'"
    15 + 22 +                               // " IGNORE xxx  LINES"
    3 + (num_fields-1)*2 + field_block_len; // " (field1, field2, ...)"
}


void Load_log_event::print_query(bool need_db, const char *cs, char *buf,
                                 char **end, char **fn_start, char **fn_end)
{
  char quoted_id[1 + NAME_LEN * 2 + 2];//quoted  length
  size_t  quoted_id_len= 0;
  char *pos= buf;

  if (need_db && db && db_len)
  {
    pos= my_stpcpy(pos, "use ");
#ifdef MYSQL_SERVER
    quoted_id_len= my_strmov_quoted_identifier(this->thd, (char *) quoted_id,
                                               db, 0);
#else
    quoted_id_len= my_strmov_quoted_identifier((char *) quoted_id, db);
#endif
    quoted_id[quoted_id_len]= '\0';
    pos= my_stpcpy(pos, quoted_id);
    pos= my_stpcpy(pos, "; ");
  }

  pos= my_stpcpy(pos, "LOAD DATA ");

  if (is_concurrent)
    pos= my_stpcpy(pos, "CONCURRENT ");

  if (fn_start)
    *fn_start= pos;

  if (check_fname_outside_temp_buf())
    pos= my_stpcpy(pos, "LOCAL ");
  pos= my_stpcpy(pos, "INFILE ");
  pos= pretty_print_str(pos, fname, fname_len);
  pos= my_stpcpy(pos, " ");

  if (sql_ex.data_info.opt_flags & REPLACE_FLAG)
    pos= my_stpcpy(pos, "REPLACE ");
  else if (sql_ex.data_info.opt_flags & IGNORE_FLAG)
    pos= my_stpcpy(pos, "IGNORE ");

  pos= my_stpcpy(pos ,"INTO");

  if (fn_end)
    *fn_end= pos;

  pos= my_stpcpy(pos ," TABLE ");
  memcpy(pos, table_name, table_name_len);
  pos+= table_name_len;

  if (cs != NULL)
  {
    pos= my_stpcpy(pos ," CHARACTER SET ");
    pos= my_stpcpy(pos ,  cs);
  }

  /* We have to create all optional fields as the default is not empty */
  pos= my_stpcpy(pos, " FIELDS TERMINATED BY ");
  pos= pretty_print_str(pos, sql_ex.data_info.field_term,
                        sql_ex.data_info.field_term_len);
  if (sql_ex.data_info.opt_flags & OPT_ENCLOSED_FLAG)
    pos= my_stpcpy(pos, " OPTIONALLY ");
  pos= my_stpcpy(pos, " ENCLOSED BY ");
  pos= pretty_print_str(pos, sql_ex.data_info.enclosed,
                        sql_ex.data_info.enclosed_len);

  pos= my_stpcpy(pos, " ESCAPED BY ");
  pos= pretty_print_str(pos, sql_ex.data_info.escaped,
                        sql_ex.data_info.escaped_len);

  pos= my_stpcpy(pos, " LINES TERMINATED BY ");
  pos= pretty_print_str(pos, sql_ex.data_info.line_term,
                        sql_ex.data_info.line_term_len);
  if (sql_ex.data_info.line_start_len)
  {
    pos= my_stpcpy(pos, " STARTING BY ");
    pos= pretty_print_str(pos, sql_ex.data_info.line_start,
                          sql_ex.data_info.line_start_len);
  }

  if ((long) skip_lines > 0)
  {
    pos= my_stpcpy(pos, " IGNORE ");
    pos= longlong10_to_str((longlong) skip_lines, pos, 10);
    pos= my_stpcpy(pos," LINES ");    
  }

  if (num_fields)
  {
    uint i;
    const char *field= fields;
    pos= my_stpcpy(pos, " (");
    for (i = 0; i < num_fields; i++)
    {
      if (i)
      {
        *pos++= ' ';
        *pos++= ',';
      }
      quoted_id_len= my_strmov_quoted_identifier(this->thd, quoted_id, field,
                                                 0);
      memcpy(pos, quoted_id, quoted_id_len-1);
    }
    *pos++= ')';
  }

  *end= pos;
}


int Load_log_event::pack_info(Protocol *protocol)
{
  char *buf, *end;

  if (!(buf= (char*) my_malloc(key_memory_log_event,
                               get_query_buffer_length(), MYF(MY_WME))))
    return 1;
  print_query(TRUE, NULL, buf, &end, 0, 0);
  protocol->store(buf, end-buf, &my_charset_bin);
  free(buf);
  return 0;
}
#endif /* defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT) */






/**
  @note
    The caller must do buf[event_len] = 0 before he starts using the
    constructed event.
*/
/*
Load_log_event::Load_log_event(const char *buf, uint event_len,
                               const Format_description_event *description_event)
: binary_log::Load_event(buf, event_len, description_event),
  Log_event(header(), footer())
{
  //DBUG_ENTER("Load_log_event");
  if (table_name != 0)
    is_valid_param= true;
  thread_id= slave_proxy_id;
  if (event_len)
  {
    
    exec_time= load_exec_time;
   
    sql_ex.data_info= sql_ex_data;
  }
  if (sql_ex.data_info.new_format())
    common_header->type_code= binary_log::NEW_LOAD_EVENT;
  else
    common_header->type_code= binary_log::LOAD_EVENT;
  
}
*/

/*
  Load_log_event::print()
*/

#ifdef MYSQL_CLIENT
void Load_log_event::print(FILE* file, PRINT_EVENT_INFO* print_event_info)
{
  print(file, print_event_info, 0);
}


void Load_log_event::print(FILE* file_arg, PRINT_EVENT_INFO* print_event_info,
			   bool commented)
{
  IO_CACHE *const head= &print_event_info->head_cache;
  size_t id_len= 0;
  char str_buf[1 + 2*FN_REFLEN + 2];

  DBUG_ENTER("Load_log_event::print");
  if (!print_event_info->short_form)
  {
    print_header(head, print_event_info, FALSE);
    my_b_printf(head, "\tQuery\tthread_id=%u\texec_time=%ld\n",
                thread_id, exec_time);
  }

  bool different_db= 1;
  if (db)
  {
    /*
      If the database is different from the one of the previous statement, we
      need to print the "use" command, and we update the last_db.
      But if commented, the "use" is going to be commented so we should not
      update the last_db.
    */
    if ((different_db= memcmp(print_event_info->db, db, db_len + 1)) &&
        !commented)
      memcpy(print_event_info->db, db, db_len + 1);
  }
  
  if (db && db[0] && different_db)
  {
#ifdef MYSQL_SERVER
    id_len= my_strmov_quoted_identifier(this->thd, str_buf, db, 0);
#else
    id_len= my_strmov_quoted_identifier(str_buf, db);
#endif
    str_buf[id_len]= '\0';
    my_b_printf(head, "%suse %s%s\n",
                commented ? "# " : "", str_buf, print_event_info->delimiter);
  }
  if (common_header->flags & LOG_EVENT_THREAD_SPECIFIC_F)
    my_b_printf(head,"%sSET @@session.pseudo_thread_id=%lu%s\n",
            commented ? "# " : "", (ulong)thread_id,
            print_event_info->delimiter);
  my_b_printf(head, "%sLOAD DATA ",
              commented ? "# " : "");
  if (check_fname_outside_temp_buf())
    my_b_printf(head, "LOCAL ");
  my_b_printf(head, "INFILE '%-*s' ", static_cast<int>(fname_len), fname);

  if (sql_ex.data_info.opt_flags & REPLACE_FLAG)
    my_b_printf(head,"REPLACE ");
  else if (sql_ex.data_info.opt_flags & IGNORE_FLAG)
    my_b_printf(head,"IGNORE ");

#ifdef MYSQL_SERVER
    id_len= my_strmov_quoted_identifier(this->thd, str_buf, table_name, 0);
#else
    id_len= my_strmov_quoted_identifier(str_buf, table_name);
#endif
  str_buf[id_len]= '\0';
  my_b_printf(head, "INTO TABLE %s", str_buf);

  my_b_printf(head, " FIELDS TERMINATED BY ");
  pretty_print_str(head, sql_ex.data_info.field_term,
                   sql_ex.data_info.field_term_len);

  if (sql_ex.data_info.opt_flags & OPT_ENCLOSED_FLAG)
    my_b_printf(head," OPTIONALLY ");
  my_b_printf(head, " ENCLOSED BY ");
  pretty_print_str(head, sql_ex.data_info.enclosed,
                   sql_ex.data_info.enclosed_len);

  my_b_printf(head, " ESCAPED BY ");
  pretty_print_str(head, sql_ex.data_info.escaped,
                   sql_ex.data_info.escaped_len);

  my_b_printf(head," LINES TERMINATED BY ");
  pretty_print_str(head, sql_ex.data_info.line_term,
                   sql_ex.data_info.line_term_len);


  if (sql_ex.data_info.line_start)
  {
    my_b_printf(head," STARTING BY ");
    pretty_print_str(head, sql_ex.data_info.line_start,
                     sql_ex.data_info.line_start_len);
  }
  if ((long) skip_lines > 0)
    my_b_printf(head, " IGNORE %ld LINES", (long) skip_lines);

  if (num_fields)
  {
    uint i;
    const char* field = fields;
    my_b_printf(head, " (");
    for (i = 0; i < num_fields; i++)
    {
      if (i)
        my_b_printf(head, ",");
      id_len= my_strmov_quoted_identifier((char *) str_buf, field);
      str_buf[id_len]= '\0';
      my_b_printf(head, "%s", str_buf);

      field += field_lens[i]  + 1;
    }
    my_b_printf(head, ")");
  }

  my_b_printf(head, "%s\n", print_event_info->delimiter);
  DBUG_VOID_RETURN;
}
#endif /* MYSQL_CLIENT */

#ifndef MYSQL_CLIENT

/**
  Load_log_event::set_fields()

  @note
    This function can not use the member variable 
    for the database, since LOAD DATA INFILE on the slave
    can be for a different database than the current one.
    This is the reason for the affected_db argument to this method.
*/
/*
void Load_log_event::set_fields(const char* affected_db, 
				List<Item> &field_list,
                                Name_resolution_context *context)
{
  uint i;
  const char* field = fields;
  for (i= 0; i < num_fields; i++)
  {
    field_list.push_back(new Item_field(context,
                                        affected_db, table_name, field));
    field+= field_lens[i]  + 1;
  }
}
*/
#endif /* !MYSQL_CLIENT */


#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
/**
  Does the data loading job when executing a LOAD DATA on the slave.

  @param net
  @param rli
  @param use_rli_only_for_errors     If set to 1, rli is provided to
                                     Load_log_event::exec_event only for this
                                     function to have rli->get_rpl_log_name and
                                     rli->last_slave_error, both being used by
                                     error reports.  If set to 0, rli is provided
                                     for full use, i.e. for error reports and
                                     position advancing.

  @todo
    fix this; this can be done by testing rules in
    Create_file_log_event::exec_event() and then discarding Append_block and
    al.
  @todo
    this is a bug - this needs to be moved to the I/O thread

  @retval
    0           Success
  @retval
    1           Failure
*/

int Load_log_event::do_apply_event(NET* net, Relay_log_info const *rli,
                                   bool use_rli_only_for_errors)
{
  DBUG_ASSERT(thd->query().str == NULL);
  thd->reset_query();                    // Should not be needed
  set_thd_db(thd, db, db_len);
  thd->is_slave_error= 0;
  clear_all_errors(thd, const_cast<Relay_log_info*>(rli));

  /* see Query_log_event::do_apply_event() and BUG#13360 */
  //DBUG_ASSERT(!rli->m_table_map.count());
  /*
    Usually lex_start() is called by mysql_parse(), but we need it here
    as the present method does not call mysql_parse().
  */
  lex_start(thd);
  thd->lex->local_file= local_fname;
  mysql_reset_thd_for_next_command(thd);

  /*
    It is possible that the thread does not hold anonymous GTID
    ownership here, e.g. in case this is the first event of a relay
    log.
  */
  gtid_reacquire_ownership_if_anonymous(thd);

   /*
    We test replicate_*_db rules. Note that we have already prepared
    the file to load, even if we are going to ignore and delete it
    now. So it is possible that we did a lot of disk writes for
    nothing. In other words, a big LOAD DATA INFILE on the master will
    still consume a lot of space on the slave (space in the relay log
    + space of temp files: twice the space of the file to load...)
    even if it will finally be ignored.  TODO: fix this; this can be
    done by testing rules in Create_file_log_event::do_apply_event()
    and then discarding Append_block and al. Another way is do the
    filtering in the I/O thread (more efficient: no disk writes at
    all).
  */
  if (rpl_filter->db_ok(thd->db().str))
  {
    thd->set_time(&(common_header->when));
    thd->set_query_id(next_query_id());
    DBUG_ASSERT(!thd->get_stmt_da()->is_set());

    TABLE_LIST tables;
    char table_buf[NAME_LEN + 1];
    my_stpcpy(table_buf, table_name);
    if (lower_case_table_names)
      my_casedn_str(system_charset_info, table_buf);
    tables.init_one_table(thd->strmake(thd->db().str, thd->db().length),
                          thd->db().length,
                          table_buf, strlen(table_buf),
                          table_buf, TL_WRITE);
    tables.updating= 1;

    // the table will be opened in mysql_load    
    if (rpl_filter->is_on() && !rpl_filter->tables_ok(thd->db().str, &tables))
    {
      // TODO: this is a bug - this needs to be moved to the I/O thread
      if (net)
        skip_load_data_infile(net);
    }
    else
    {
      char llbuff[22];
      char *end;
      enum enum_duplicates handle_dup;
      char *load_data_query;

      /*
        Forge LOAD DATA INFILE query which will be used in SHOW PROCESS LIST
        and written to slave's binlog if binlogging is on.
      */
      if (!(load_data_query= (char *)thd->alloc(get_query_buffer_length() + 1)))
      {
        /*
          This will set thd->fatal_error in case of OOM. So we surely will notice
          that something is wrong.
        */
        goto error;
      }

      print_query(FALSE, NULL, load_data_query, &end, NULL, NULL);
      *end= 0;
      thd->set_query(load_data_query, static_cast<size_t>(end - load_data_query));

      if (sql_ex.data_info.opt_flags & REPLACE_FLAG)
        handle_dup= DUP_REPLACE;
      else if (sql_ex.data_info.opt_flags & IGNORE_FLAG)
      {
        thd->lex->set_ignore(true);
        handle_dup= DUP_ERROR;
      }
      else
      {
        /*
          When replication is running fine, if it was DUP_ERROR on the
          master then we could choose IGNORE here, because if DUP_ERROR
          suceeded on master, and data is identical on the master and slave,
          then there should be no uniqueness errors on slave, so IGNORE is
          the same as DUP_ERROR. But in the unlikely case of uniqueness errors
          (because the data on the master and slave happen to be different
          (user error or bug), we want LOAD DATA to print an error message on
          the slave to discover the problem.

          If reading from net (a 3.23 master), mysql_load() will change this
          to IGNORE.
        */
        handle_dup= DUP_ERROR;
      }
      /*
        We need to set thd->lex->sql_command and thd->lex->duplicates
        since InnoDB tests these variables to decide if this is a LOAD
        DATA ... REPLACE INTO ... statement even though mysql_parse()
        is not called.  This is not needed in 5.0 since there the LOAD
        DATA ... statement is replicated using mysql_parse(), which
        sets the thd->lex fields correctly.
      */
      thd->lex->sql_command= SQLCOM_LOAD;
      thd->lex->duplicates= handle_dup;

      sql_exchange ex((char*)fname, sql_ex.data_info.opt_flags & DUMPFILE_FLAG);
      String field_term(sql_ex.data_info.field_term,
                        sql_ex.data_info.field_term_len,log_cs);
      String enclosed(sql_ex.data_info.enclosed,
                      sql_ex.data_info.enclosed_len,log_cs);
      String line_term(sql_ex.data_info.line_term,
                       sql_ex.data_info.line_term_len,log_cs);
      String line_start(sql_ex.data_info.line_start,
                        sql_ex.data_info.line_start_len,log_cs);
      String escaped(sql_ex.data_info.escaped,
                     sql_ex.data_info.escaped_len, log_cs);
      const String empty_str("", 0, log_cs);
      ex.field.field_term= &field_term;
      ex.field.enclosed= &enclosed;
      ex.line.line_term= &line_term;
      ex.line.line_start= &line_start;
      ex.field.escaped= &escaped;

      ex.field.opt_enclosed= (sql_ex.data_info.opt_flags & OPT_ENCLOSED_FLAG);
      if (sql_ex.data_info.empty_flags & FIELD_TERM_EMPTY)
        ex.field.field_term= &empty_str;

      ex.skip_lines= skip_lines;
      List<Item> field_list;
      thd->lex->select_lex->context.resolve_in_table_list_only(&tables);
      set_fields(tables.db, field_list, &thd->lex->select_lex->context);
      thd->variables.pseudo_thread_id= thread_id;
      if (net)
      {
        // mysql_load will use thd->net to read the file
        thd->get_protocol_classic()->set_vio(net->vio);
        // Make sure the client does not get confused about the packet sequence
        thd->get_protocol_classic()->set_pkt_nr(net->pkt_nr);
      }
      /*
        It is safe to use tmp_list twice because we are not going to
        update it inside mysql_load().
      */
      List<Item> tmp_list;
      /*
        Prepare column privilege check for LOAD statement.
        This is necessary because the replication code for LOAD bypasses
        regular privilege checking, which is done by check_one_table_access()
        in regular code path.
        We can assign INSERT privileges to the table since the slave thread
        operates with all privileges.
      */
      tables.set_privileges(INSERT_ACL);
      tables.set_want_privilege(INSERT_ACL);

      if (open_temporary_tables(thd, &tables) ||
          mysql_load(thd, &ex, &tables, field_list, tmp_list, tmp_list,
                     handle_dup, net != 0))
        thd->is_slave_error= 1;
      if (thd->cuted_fields)
      {
        /* log_pos is the position of the LOAD event in the master log */
        sql_print_warning("Slave: load data infile on table '%s' at "
                          "log position %s in log '%s' produced %ld "
                          "warning(s). Default database: '%s'",
                          (char*) table_name,
                          llstr(common_header->log_pos,llbuff),
                          const_cast<Relay_log_info*>(rli)->get_rpl_log_name(),
                          (ulong) thd->cuted_fields,
                          print_slave_db_safe(thd->db().str));
      }
      if (net)
      {
        net->pkt_nr= thd->get_protocol_classic()->get_pkt_nr();
      }
    }
  }
  else
  {
    /*
      We will just ask the master to send us /dev/null if we do not
      want to load the data.
      TODO: this a bug - needs to be done in I/O thread
    */
    if (net)
      skip_load_data_infile(net);
  }

error:
  thd->get_protocol_classic()->set_vio(NULL);
  const char *remember_db= thd->db().str;
  thd->set_catalog(NULL_CSTR);
  thd->set_db(NULL_CSTR);                   /* will free the current database */
  thd->reset_query();
  thd->get_stmt_da()->set_overwrite_status(true);
  thd->is_error() ? trans_rollback_stmt(thd) : trans_commit_stmt(thd);
  thd->get_stmt_da()->set_overwrite_status(false);
  close_thread_tables(thd);
  /*
    - If transaction rollback was requested due to deadlock
      perform it and release metadata locks.
    - If inside a multi-statement transaction,
    defer the release of metadata locks until the current
    transaction is either committed or rolled back. This prevents
    other statements from modifying the table for the entire
    duration of this transaction.  This provides commit ordering
    and guarantees serializability across multiple transactions.
    - If in autocommit mode, or outside a transactional context,
    automatically release metadata locks of the current statement.
  */
  if (thd->transaction_rollback_request)
  {
    trans_rollback_implicit(thd);
    thd->mdl_context.release_transactional_locks();
  }
  else if (! thd->in_multi_stmt_transaction_mode())
    thd->mdl_context.release_transactional_locks();
  else
    thd->mdl_context.release_statement_locks();

  DBUG_EXECUTE_IF("LOAD_DATA_INFILE_has_fatal_error",
                  thd->is_slave_error= 0; thd->is_fatal_error= 1;);

  if (thd->is_slave_error)
  {
    /* this err/sql_errno code is copy-paste from net_send_error() */
    const char *err;
    int sql_errno;
    if (thd->is_error())
    {
      err= thd->get_stmt_da()->message_text();
      sql_errno= thd->get_stmt_da()->mysql_errno();
    }
    else
    {
      sql_errno=ER_UNKNOWN_ERROR;
      err=ER(sql_errno);       
    }
    rli->report(ERROR_LEVEL, sql_errno,"\
Error '%s' running LOAD DATA INFILE on table '%s'. Default database: '%s'",
                    err, (char*)table_name, print_slave_db_safe(remember_db));
    free_root(thd->mem_root,MYF(MY_KEEP_PREALLOC));
    return 1;
  }
  free_root(thd->mem_root,MYF(MY_KEEP_PREALLOC));

  if (thd->is_fatal_error)
  {
    char buf[256];
    my_snprintf(buf, sizeof(buf),
                "Running LOAD DATA INFILE on table '%-.64s'."
                " Default database: '%-.64s'",
                (char*)table_name,
                print_slave_db_safe(remember_db));

    rli->report(ERROR_LEVEL, ER_SLAVE_FATAL_ERROR,
                ER(ER_SLAVE_FATAL_ERROR), buf);
    return 1;
  }

  return ( use_rli_only_for_errors ? 0 : Log_event::do_apply_event(rli) ); 
}
#endif


/**************************************************************************
  Rotate_log_event methods
**************************************************************************/
Rotate_log_event::Rotate_log_event(const char* buf, uint event_len,
                                   const Format_description_event* description_event)
: binary_log::Rotate_event(buf, event_len, description_event),
  Log_event(header(), footer())
{
  
  if (new_log_ident != 0)
    is_valid_param= true;

}
/*
  Rotate_log_event::pack_info()
*/

#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
int Rotate_log_event::pack_info(Protocol *protocol)
{
  char buf1[256], buf[22];
  String tmp(buf1, sizeof(buf1), log_cs);
  tmp.length(0);
  tmp.append(new_log_ident, ident_len);
  tmp.append(STRING_WITH_LEN(";pos="));
  tmp.append(llstr(pos,buf));
  protocol->store(tmp.ptr(), tmp.length(), &my_charset_bin);
  return 0;
}
#endif


/*
  Rotate_log_event::print()
*/

#ifdef MYSQL_CLIENT
void Rotate_log_event::print(FILE* file, PRINT_EVENT_INFO* print_event_info)
{
  char buf[22];
  IO_CACHE *const head= &print_event_info->head_cache;

  if (print_event_info->short_form)
    return;
  print_header(head, print_event_info, FALSE);
  my_b_printf(head, "\tRotate to ");
  if (new_log_ident)
    my_b_write(head, (uchar*) new_log_ident, (uint)ident_len);
  my_b_printf(head, "  pos: %s\n", llstr(pos, buf));
}
#endif /* MYSQL_CLIENT */


#if defined( HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
int Begin_load_query_log_event::get_create_or_append() const
{
  return 1; /* create the file */
}
#endif /* defined( HAVE_REPLICATION) && !defined(MYSQL_CLIENT) */


#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
Log_event::enum_skip_reason
Begin_load_query_log_event::do_shall_skip(Relay_log_info *rli)
{
  /*
    If the slave skip counter is 1, then we should not start executing
    on the next event.
  */
  return continue_group(rli);
}
#endif

#ifdef MYSQL_CLIENT
void Execute_load_query_log_event::print(FILE* file,
                                         PRINT_EVENT_INFO* print_event_info)
{
  print(file, print_event_info, 0);
}

/**
  Prints the query as LOAD DATA LOCAL and with rewritten filename.
*/
void Execute_load_query_log_event::print(FILE* file,
                                         PRINT_EVENT_INFO* print_event_info,
                                         const char *local_fname)
{
  IO_CACHE *const head= &print_event_info->head_cache;

  print_query_header(head, print_event_info);
  /**
    reduce the size of io cache so that the write function is called
    for every call to my_b_printf().
   */
  DBUG_EXECUTE_IF ("simulate_execute_event_write_error",
                   {head->write_pos= head->write_end;
                   DBUG_SET("+d,simulate_file_write_error");});

  if (local_fname)
  {
    my_b_write(head, (uchar*) query, fn_pos_start);
    my_b_printf(head, " LOCAL INFILE ");
    pretty_print_str(head, local_fname, strlen(local_fname));

    if (dup_handling == binary_log::LOAD_DUP_REPLACE)
      my_b_printf(head, " REPLACE");
    my_b_printf(head, " INTO");
    my_b_write(head, (uchar*) query + fn_pos_end, q_len-fn_pos_end);
    my_b_printf(head, "\n%s\n", print_event_info->delimiter);
  }
  else
  {
    my_b_write(head, (uchar*) query, q_len);
    my_b_printf(head, "\n%s\n", print_event_info->delimiter);
  }

  if (!print_event_info->short_form)
    my_b_printf(head, "# file_id: %d \n", file_id);
}
#endif


#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
int Execute_load_query_log_event::pack_info(Protocol *protocol)
{
  char *buf, *pos;
  if (!(buf= (char*) my_malloc(key_memory_log_event,
                               9 + (db_len * 2) + 2 + q_len + 10 + 21,
                               MYF(MY_WME))))
    return 1;
  pos= buf;
  if (db && db_len)
  {
    /*
      Statically allocates room to store '\0' and an identifier
      that may have NAME_LEN * 2 due to quoting and there are
      two quoting characters that wrap them.
    */
    char quoted_db[1 + NAME_LEN * 2 + 2];// quoted length of the identifier
    size_t size= 0;
    size= my_strmov_quoted_identifier(this->thd, quoted_db, db, 0);
    pos= my_stpcpy(buf, "use ");
    memcpy(pos, quoted_db, size);
    pos= my_stpcpy(pos + size, "; ");
  }
  if (query && q_len)
  {
    memcpy(pos, query, q_len);
    pos+= q_len;
  }
  pos= my_stpcpy(pos, " ;file_id=");
  pos= int10_to_str((long) file_id, pos, 10);
  protocol->store(buf, pos-buf, &my_charset_bin);
  free(buf);
  return 0;
}


int
Execute_load_query_log_event::do_apply_event(Relay_log_info const *rli)
{
  char *p;
  char *buf;
  char *fname;
  char *fname_end;
  int error;

  buf= (char*) my_malloc(key_memory_log_event,
                         q_len + 1 - (fn_pos_end - fn_pos_start) +
                         (FN_REFLEN + TEMP_FILE_MAX_LEN) + 10 + 8 + 5, MYF(MY_WME));

  DBUG_EXECUTE_IF("LOAD_DATA_INFILE_has_fatal_error", my_free(buf); buf= NULL;);

  /* Replace filename and LOCAL keyword in query before executing it */
  if (buf == NULL)
  {
    rli->report(ERROR_LEVEL, ER_SLAVE_FATAL_ERROR,
                ER(ER_SLAVE_FATAL_ERROR), "Not enough memory");
    return 1;
  }

  p= buf;
  memcpy(p, query, fn_pos_start);
  p+= fn_pos_start;
  fname= (p= strmake(p, STRING_WITH_LEN(" INFILE \'")));
  p= slave_load_file_stem(p, file_id, server_id, ".data");
  fname_end= p= strend(p);                      // Safer than p=p+5
  *(p++)='\'';
  switch (dup_handling) {
  case binary_log::LOAD_DUP_IGNORE:
    p= strmake(p, STRING_WITH_LEN(" IGNORE"));
    break;
  case binary_log::LOAD_DUP_REPLACE:
    p= strmake(p, STRING_WITH_LEN(" REPLACE"));
    break;
  default:
    /* Ordinary load data */
    break;
  }
  p= strmake(p, STRING_WITH_LEN(" INTO "));
  p= strmake(p, query+fn_pos_end, q_len-fn_pos_end);

  error= Query_log_event::do_apply_event(rli, buf, p-buf);

  /* Forging file name for deletion in same buffer */
  *fname_end= 0;

  /*
    If there was an error the slave is going to stop, leave the
    file so that we can re-execute this event at START SLAVE.
  */
  if (!error)
    mysql_file_delete(key_file_log_event_data, fname, MYF(MY_WME));

  free(buf);
  return error;
}
#endif


/**************************************************************************
	sql_ex_info methods
**************************************************************************/

/*
  sql_ex_info::write_data()
*/

/*
bool sql_ex_info::write_data(IO_CACHE* file)
{
  if (data_info.new_format())
  {
    return (write_str_at_most_255_bytes(file, data_info.field_term,
                                        (uint) data_info.field_term_len) ||
	    write_str_at_most_255_bytes(file, data_info.enclosed,
                                        (uint) data_info.enclosed_len) ||
	    write_str_at_most_255_bytes(file, data_info.line_term,
                                        (uint) data_info.line_term_len) ||
	    write_str_at_most_255_bytes(file, data_info.line_start,
                                        (uint) data_info.line_start_len) ||
	    write_str_at_most_255_bytes(file, data_info.escaped,
                                        (uint) data_info.escaped_len) ||
	    my_b_safe_write(file,(uchar*) &(data_info.opt_flags), 1));
  }
  else
  {
    binary_log::old_sql_ex old_ex;
    old_ex.field_term= *(data_info.field_term);
    old_ex.enclosed=   *(data_info.enclosed);
    old_ex.line_term=  *(data_info.line_term);
    old_ex.line_start= *(data_info.line_start);
    old_ex.escaped=    *(data_info.escaped);
    old_ex.opt_flags=  data_info.opt_flags;
    old_ex.empty_flags= data_info.empty_flags;
    return my_b_safe_write(file, (uchar*) &old_ex, sizeof(old_ex)) != 0;
  }
}
*/

/**
  sql_ex_info::init()
  This method initializes the members of strcuture variable sql_ex_info,
  defined in a Load_log_event. The structure, initializes the sub struct
  data_info, with the subclause characters in a LOAD_DATA_INFILE query.

*/
/*
const char *sql_ex_info::init(const char *buf, const char *buf_end,
                              bool use_new_format)
{
  return data_info.init(buf, buf_end, use_new_format);
}
*/
#ifndef DBUG_OFF
#ifndef MYSQL_CLIENT
static uchar dbug_extra_row_data_val= 0;

/**
   set_extra_data

   Called during self-test to generate various
   self-consistent binlog row event extra
   thread data structures which can be checked
   when reading the binlog.

   @param arr  Buffer to use
*/
const uchar* set_extra_data(uchar* arr)
{
  uchar val= (dbug_extra_row_data_val++) %
    (EXTRA_ROW_INFO_MAX_PAYLOAD + 1); /* 0 .. MAX_PAYLOAD + 1 */
  arr[EXTRA_ROW_INFO_LEN_OFFSET]= val + EXTRA_ROW_INFO_HDR_BYTES;
  arr[EXTRA_ROW_INFO_FORMAT_OFFSET]= val;
  for (uchar i=0; i<val; i++)
    arr[EXTRA_ROW_INFO_HDR_BYTES+i]= val;

  return arr;
}

#endif // #ifndef MYSQL_CLIENT

/**
   check_extra_data

   Called during self-test to check that
   binlog row event extra data is self-
   consistent as defined by the set_extra_data
   function above.

   Will assert(false) if not.

   @param extra_row_data
*/
void check_extra_data(uchar* extra_row_data)
{
  assert(extra_row_data);
  uint16 len= extra_row_data[EXTRA_ROW_INFO_LEN_OFFSET];
  uint8 val= len - EXTRA_ROW_INFO_HDR_BYTES;
  assert(extra_row_data[EXTRA_ROW_INFO_FORMAT_OFFSET] == val);
  for (uint16 i= 0; i < val; i++)
  {
    assert(extra_row_data[EXTRA_ROW_INFO_HDR_BYTES + i] == val);
  }
}

#endif  // #ifndef DBUG_OFF



enum enum_tbl_map_status
{
  /* no duplicate identifier found */
  OK_TO_PROCESS= 0,

  /* this table map must be filtered out */
  FILTERED_OUT= 1,

  /* identifier mapping table with different properties */
  SAME_ID_MAPPING_DIFFERENT_TABLE= 2,
  
  /* a duplicate identifier was found mapping the same table */
  SAME_ID_MAPPING_SAME_TABLE= 3,

  /*
    this table must be filtered out but found an active XA transaction. XA
    transactions shouldn't be used with replication filters, until disabling
    the XA read only optimization is a supported feature.
  */
  FILTERED_WITH_XA_ACTIVE = 4
};


Rows_query_log_event::Rows_query_log_event(const char *buf, uint event_len,
                                           const Format_description_event
                                           *descr_event)
  : binary_log::Ignorable_event(buf, descr_event),
    Ignorable_log_event(buf, descr_event),
    binary_log::Rows_query_event(buf, event_len, descr_event)
{
  is_valid_param= (m_rows_query != NULL);
}

Ignorable_log_event::Ignorable_log_event(const char *buf,
                                         const Format_description_event *descr_event)
  : binary_log::Ignorable_event(buf, descr_event),
    Log_event(header(), footer())
{
  is_valid_param= true;
}

Ignorable_log_event::~Ignorable_log_event()
{
}

bool
Rows_query_log_event::write_data_body(IO_CACHE *file)
{

  /*
   m_rows_query length will be stored using only one byte, but on read
   that length will be ignored and the complete query will be read.
  */
  return write_str_at_most_255_bytes(file, m_rows_query,strlen(m_rows_query));
}

void my_free(void *ptr)
{
  free(ptr);
}

Table_map_log_event::Table_map_log_event(const char *buf, uint event_len,
                                         const Format_description_event
                                         *description_event)

   : binary_log::Table_map_event(buf, event_len, description_event),
     Log_event(header(), footer())
{

  if (m_null_bits != NULL && m_field_metadata != NULL && m_coltype != NULL)
    is_valid_param= true;

}

Table_map_log_event::~Table_map_log_event()
{
  if(m_null_bits)
  {
    my_free(m_null_bits);
    m_null_bits= NULL;
  }
  if(m_field_metadata)
  {
    my_free(m_field_metadata);
    m_field_metadata= NULL;
  }
}

void* my_multi_malloc( myf myFlags, ...)
{
  va_list args;
  char **ptr,*start,*res;
  size_t tot_length,length;


  va_start(args,myFlags);
  tot_length=0;
  while ((ptr=va_arg(args, char **)))
  {
    length=va_arg(args,uint);
    tot_length+=ALIGN_SIZE(length);
  }
  va_end(args);

  if (!(start=(char *) malloc( tot_length)))
    return(0); 

  va_start(args,myFlags);
  res=start;
  while ((ptr=va_arg(args, char **)))
  {
    *ptr=res;
    length=va_arg(args,uint);
    res+=ALIGN_SIZE(length);
  }
  va_end(args);
  return ((void*) start);
}

table_def::table_def(unsigned char *types, ulong size,
                     uchar *field_metadata, int metadata_size,
                     uchar *null_bitmap, uint16 flags)
  : m_size(size), m_type(0), m_field_metadata_size(metadata_size),
    m_field_metadata(0), m_null_bits(0), m_flags(flags),
    m_memory(NULL)
{
  m_memory= (uchar *)my_multi_malloc(16,
                                     &m_type, size,
                                     &m_field_metadata,
                                     size * sizeof(uint16),
                                     &m_null_bits, (size + 7) / 8,
                                     NULL);

  memset(m_field_metadata, 0, size * sizeof(uint16));

  if (m_type)
    memcpy(m_type, types, size);
  else
    m_size= 0;
  /*
    Extract the data from the table map into the field metadata array
    iff there is field metadata. The variable metadata_size will be
    0 if we are replicating from an older version server since no field
    metadata was written to the table map. This can also happen if 
    there were no fields in the master that needed extra metadata.
  */
  if (m_size && metadata_size)
  { 
    int index= 0;
    for (unsigned int i= 0; i < m_size; i++)
    {
      switch (binlog_type(i)) {
      case MYSQL_TYPE_TINY_BLOB_B:
      case MYSQL_TYPE_BLOB_B:
      case MYSQL_TYPE_MEDIUM_BLOB_B:
      case MYSQL_TYPE_LONG_BLOB_B:
      case MYSQL_TYPE_DOUBLE_B:
      case MYSQL_TYPE_FLOAT_B:
      case MYSQL_TYPE_GEOMETRY_B:
      case MYSQL_TYPE_JSON_B:
      {
        /*
          These types store a single byte.
        */
        m_field_metadata[i]= field_metadata[index];
        index++;
        break;
      }
      case MYSQL_TYPE_SET_B:
      case MYSQL_TYPE_ENUM_B:
      case MYSQL_TYPE_STRING_B:
      {
        uint16 x= field_metadata[index++] << 8U; // real_type
        x+= field_metadata[index++];            // pack or field length
        m_field_metadata[i]= x;
        break;
      }
      case MYSQL_TYPE_BIT_B:
      {
        uint16 x= field_metadata[index++];
        x = x + (field_metadata[index++] << 8U);
        m_field_metadata[i]= x;
        break;
      }
      case MYSQL_TYPE_VARCHAR_B:
      {
        /*
          These types store two bytes.
        */
        char *ptr= (char *)&field_metadata[index];
        m_field_metadata[i]= uint2korr(ptr);
        index= index + 2;
        break;
      }
      case MYSQL_TYPE_NEWDECIMAL_B:
      {
        uint16 x= field_metadata[index++] << 8U; // precision
        x+= field_metadata[index++];            // decimals
        m_field_metadata[i]= x;
        break;
      }
      case MYSQL_TYPE_TIME2_B:
      case MYSQL_TYPE_DATETIME2_B:
      case MYSQL_TYPE_TIMESTAMP2_B:
        m_field_metadata[i]= field_metadata[index++];
        break;
      default:
        m_field_metadata[i]= 0;
        break;
      }
    }
  }
  if (m_size && null_bitmap)
    memcpy(m_null_bits, null_bitmap, (m_size + 7) / 8);
}

int decimal_binary_size(int precision, int scale)
 {
   static const int dig2bytes[10]= {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
   int intg= precision-scale,
       intg0= intg/9, frac0= scale/9,
       intg0x= intg-intg0*9, frac0x= scale-frac0*9;

   return intg0 * sizeof(uint32_t) + dig2bytes[intg0x]+
          frac0 * sizeof(uint32_t) + dig2bytes[frac0x];
 }

unsigned int my_time_binary_length(unsigned int dec)
{
  return 3 + (dec + 1) / 2;
}

static unsigned int uint_max(int bits) {
  return (((1U << (bits - 1)) - 1) << 1) | 1;
}
/**
   Compute the maximum display length of a field.

   @param sql_type Type of the field
   @param metadata The metadata from the master for the field.
   @return Maximum length of the field in bytes.
 */
unsigned int
max_display_length_for_field(enum_field_types_b sql_type, unsigned int metadata)
{
  switch (sql_type) {
  case MYSQL_TYPE_NEWDECIMAL_B:
    return metadata >> 8;

  case MYSQL_TYPE_FLOAT_B:
    return 12;

  case MYSQL_TYPE_DOUBLE_B:
    return 22;

  case MYSQL_TYPE_SET_B:
  case MYSQL_TYPE_ENUM_B:
      return metadata & 0x00ff;

  case MYSQL_TYPE_STRING_B:
  {
    unsigned char type= metadata >> 8;
    if (type == MYSQL_TYPE_SET_B || type == MYSQL_TYPE_ENUM_B)
      return metadata & 0xff;
    else
      return (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00ff);
  }

  case MYSQL_TYPE_YEAR_B:
  case MYSQL_TYPE_TINY_B:
    return 4;

  case MYSQL_TYPE_SHORT_B:
    return 6;

  case MYSQL_TYPE_INT24_B:
    return 9;

  case MYSQL_TYPE_LONG_B:
    return 11;

  case MYSQL_TYPE_LONGLONG_B:
    return 20;

  case MYSQL_TYPE_NULL_B:
    return 0;

  case MYSQL_TYPE_NEWDATE_B:
    return 3;

  case MYSQL_TYPE_DATE_B:
  case MYSQL_TYPE_TIME_B:
  case MYSQL_TYPE_TIME2_B:
    return 3;

  case MYSQL_TYPE_TIMESTAMP_B:
  case MYSQL_TYPE_TIMESTAMP2_B:
    return 4;

  case MYSQL_TYPE_DATETIME_B:
  case MYSQL_TYPE_DATETIME2_B:
    return 8;

  case MYSQL_TYPE_BIT_B:
    /*
      Decode the size of the bit field from the master.
    */
  
    return 8 * (metadata >> 8U) + (metadata & 0x00ff);

  case MYSQL_TYPE_VAR_STRING_B:
  case MYSQL_TYPE_VARCHAR_B:
    return metadata;

    /*
      The actual length for these types does not really matter since
      they are used to calc_pack_length, which ignores the given
      length for these types.

      Since we want this to be accurate for other uses, we return the
      maximum size in bytes of these BLOBs.
    */

  case MYSQL_TYPE_TINY_BLOB_B:
    return uint_max(1 * 8);

  case MYSQL_TYPE_MEDIUM_BLOB_B:
    return uint_max(3 * 8);

  case MYSQL_TYPE_BLOB_B:
    /*
      For the blob type, Field::real_type() lies and say that all
      blobs are of type MYSQL_TYPE_BLOB. In that case, we have to look
      at the length instead to decide what the max display size is.
     */
    return uint_max(metadata * 8);

  case MYSQL_TYPE_LONG_BLOB_B:
  case MYSQL_TYPE_GEOMETRY_B:
  case MYSQL_TYPE_JSON_B:
    return uint_max(4 * 8);

  default:
    return UINT_MAX;
  }
}
uint32_t inline le32toh(uint32_t x)
{
    /*
    return (((x >> 24) & 0xff) |
            ((x <<  8) & 0xff0000) |
            ((x >>  8) & 0xff00) |
            ((x << 24) & 0xff000000));
    */        
   return x;
}

unsigned int my_timestamp_binary_length(unsigned int dec)
{
  return 4 + (dec + 1) / 2;
}

unsigned int my_datetime_binary_length(unsigned int dec)
{
  return 5 + (dec + 1) / 2;
}


/**
 This helper function calculates the size in bytes of a particular field in a
 row type event as defined by the field_ptr and metadata_ptr arguments.
 @param col Field type code
 @param master_data The field data
 @param metadata The field metadata

 @return The size in bytes of a particular field
*/
uint32_t calc_field_size(unsigned char col, const unsigned char *master_data,
                         unsigned int metadata)
{
  uint32_t length= 0;

  switch ((col)) {
  case MYSQL_TYPE_NEWDECIMAL_B:
    length= decimal_binary_size(metadata >> 8,
                                metadata & 0xff);
    break;
  case MYSQL_TYPE_DECIMAL_B:
  case MYSQL_TYPE_FLOAT_B:
  case MYSQL_TYPE_DOUBLE_B:
    length= metadata;
    break;
  /*
    The cases for SET and ENUM are include for completeness, however
    both are mapped to type MYSQL_TYPE_STRING and their real types
    are encoded in the field metadata.
  */
  case MYSQL_TYPE_SET_B:
  case MYSQL_TYPE_ENUM_B:
  case MYSQL_TYPE_STRING_B:
  {
    unsigned char type= metadata >> 8U;
    if ((type == MYSQL_TYPE_SET_B) || (type == MYSQL_TYPE_ENUM_B))
      length= metadata & 0x00ff;
    else
    {
      /*
        We are reading the actual size from the master_data record
        because this field has the actual lengh stored in the first
        one or two bytes.
      */
      length= max_display_length_for_field(MYSQL_TYPE_STRING_B, metadata) > 255 ? 2 : 1;

      if (length == 1)
        length+= *master_data;
      else
      {
        uint32_t temp= 0;
        memcpy(&temp, master_data, 2);
        length= length + le32toh(temp);
      }
    }
    break;
  }
  case MYSQL_TYPE_YEAR_B:
  case MYSQL_TYPE_TINY_B:
    length= 1;
    break;
  case MYSQL_TYPE_SHORT_B:
    length= 2;
    break;
  case MYSQL_TYPE_INT24_B:
    length= 3;
    break;
  case MYSQL_TYPE_LONG_B:
    length= 4;
    break;
  case MYSQL_TYPE_LONGLONG_B:
    length= 8;
    break;
  case MYSQL_TYPE_NULL_B:
    length= 0;
    break;
  case MYSQL_TYPE_NEWDATE_B:
    length= 3;
    break;
  case MYSQL_TYPE_DATE_B:
  case MYSQL_TYPE_TIME_B:
    length= 3;
    break;
  case MYSQL_TYPE_TIME2_B:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in my_time.c, the signature
      being  unsigned int my_time_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    length= my_time_binary_length(metadata);
    break;
  case MYSQL_TYPE_TIMESTAMP_B:
    length= 4;
    break;
  case MYSQL_TYPE_TIMESTAMP2_B:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in time.c, the signature
      being  unsigned int my_timestamp_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    length= my_timestamp_binary_length(metadata);
    break;
  case MYSQL_TYPE_DATETIME_B:
    length= 8;
    break;
  case MYSQL_TYPE_DATETIME2_B:
    /*
      The original methods in the server to calculate the binary size of the
      packed numeric time representation is defined in time.c, the signature
      being  unsigned int my_datetime_binary_length(uint)

      The length below needs to be updated if the above method is updated in
      the server
    */
    length= my_datetime_binary_length(metadata);
    break;
  case MYSQL_TYPE_BIT_B:
  {
    /*
      Decode the size of the bit field from the master.
        from_len is the length in bytes from the master
        from_bit_len is the number of extra bits stored in the master record
      If from_bit_len is not 0, add 1 to the length to account for accurate
      number of bytes needed.
    */
    unsigned int from_len= (metadata >> 8U) & 0x00ff;
    unsigned int from_bit_len= metadata & 0x00ff;

    length= from_len + ((from_bit_len > 0) ? 1 : 0);
    break;
  }
  case MYSQL_TYPE_VARCHAR_B:
  {
    length= metadata > 255 ? 2 : 1;
    if (length == 1)
      length+= (uint32_t) *master_data;
    else
    {
      uint32_t temp= 0;
      memcpy(&temp, master_data, 2);
      length= length + le32toh(temp);
    }
    break;
  }
  case MYSQL_TYPE_TINY_BLOB_B:
  case MYSQL_TYPE_MEDIUM_BLOB_B:
  case MYSQL_TYPE_LONG_BLOB_B:
  case MYSQL_TYPE_BLOB_B:
  case MYSQL_TYPE_GEOMETRY_B:
  case MYSQL_TYPE_JSON_B:
  {
    /*
      Compute the length of the data. We cannot use get_length() here
      since it is dependent on the specific table (and also checks the
      packlength using the internal 'table' pointer) and replication
      is using a fixed format for storing data in the binlog.
    */
    switch (metadata) {
    case 1:
      length= *master_data;
      break;
    case 2:
      memcpy(&length, master_data, 2);
      length= le32toh(length);
      break;
    case 3:
      memcpy(&length, master_data, 3);
      length= le32toh(length);
      break;
    case 4:
      memcpy(&length, master_data, 4);
      length= le32toh(length);
      break;
    default:
      break;
    }

    length+= metadata;
    break;
  }
  default:
    length= UINT_MAX;
  }
  return length;
}
uint32 table_def::calc_field_size(uint col, uchar *master_data) const
{
  uint32 length= ::calc_field_size(type(col), master_data,
                                   m_field_metadata[col]);
  return length;
}

table_def::~table_def()
{
  my_free(m_memory);
#ifndef DBUG_OFF
  m_type= 0;
  m_size= 0;
#endif
}
