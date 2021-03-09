
#ifndef _log_event_h
#define _log_event_h

#include <list>
#include <map>
#include <set>
#include <string>
#include <pthread.h>
#include "binlog_event.h"
#include "statement_events.h"
#include "rows_event.h"
#include "typelib.h"

typedef unsigned long long int ulonglong; /* ulong or unsigned long long */
typedef long long int	longlong;
typedef longlong int64;
typedef ulonglong uint64;
typedef unsigned long	ulong;

#define MAX_DBS_IN_EVENT_MTS 16
#define	ULLONG_MAX	0xffffffffffffffffULL	/* max unsigned long long */
#define	LLONG_MAX	0x7fffffffffffffffLL	/* max signed long long */
/* General constants */
#define FN_LEN		256	/* Max file name len */
#define FN_HEADLEN	253	/* Max length of filepart of file name */
#define FN_EXTLEN	20	/* Max length of extension (part of FN_LEN) */
#define FN_REFLEN	512	/* Max length of full path-name */
#define FN_REFLEN_SE	4000	/* Max length of full path-name in SE */
#define FN_EXTCHAR	'.'
#define FN_HOMELIB	'~'	/* ~/ is used as abbrev for home dir */
#define FN_CURLIB	'.'	/* ./ is used as abbrev for current dir */
#define FN_PARENTDIR	".."	/* Parent directory; Must be a string */
#define STRING_WITH_LEN(X) (X), ((sizeof(X) - 1))
typedef unsigned char	uchar;	/* Short for unsigned char */
typedef signed char int8;       /* Signed integer >= 8  bits */
typedef unsigned char uint8;    /* Unsigned integer >= 8  bits */
typedef short int16;
typedef unsigned short uint16;
typedef int rpl_sidno;
typedef long long int rpl_gno;


typedef long int32;
typedef unsigned int uint32;
typedef unsigned long my_off_t;
typedef int	File;		/* File descriptor */
typedef unsigned int PSI_file_key;
typedef int (*IO_CACHE_CALLBACK)(struct st_io_cache*);
typedef int		myf;	/* Type of MyFlags in my_funcs */
typedef char my_bool;
typedef uint32 ha_checksum;
#define MAX_TIME_ZONE_NAME_LENGTH       64
#define my_b_inited(info) (info)->buffer
class Format_description_log_event;
typedef bool (*read_log_event_filter_function)(char** buf,
                                               ulong*,
                                               const Format_description_log_event*);

inline uint16 uint2korr(const uchar *A) { return *((uint16*) A); }

inline uint16    uint2korr(const char *pT)
{
  return uint2korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}

using binary_log::enum_binlog_checksum_alg;
using binary_log::checksum_crc32;
using binary_log::Log_event_type;
using binary_log::Log_event_header;
using binary_log::Log_event_footer;
using binary_log::Binary_log_event;
using binary_log::Format_description_event;

typedef uint32 my_bitmap_map;

typedef struct st_bitmap
{
  my_bitmap_map *bitmap;
  uint n_bits; /* number of bits occupied by the above */
  my_bitmap_map last_word_mask;
  my_bitmap_map *last_word_ptr;
  pthread_mutex_t *mutex;
} MY_BITMAP;

static inline int native_strncasecmp(const char *s1, const char *s2, size_t n)
{
    return strncasecmp(s1, s2, n);
}

void my_free(void *ptr);


enum cache_type
{
  TYPE_NOT_SET= 0, READ_CACHE, WRITE_CACHE,
  SEQ_READ_APPEND		/* sequential read or append */,
  READ_FIFO, READ_NET,WRITE_NET};

typedef struct st_io_cache_share
{
  pthread_mutex_t       mutex;           /* To sync on reads into buffer. */
  pthread_cond_t        cond;            /* To wait for signals. */
  pthread_cond_t        cond_writer;     /* For a synchronized writer. */
  /* Offset in file corresponding to the first byte of buffer. */
  my_off_t              pos_in_file;
  /* If a synchronized write cache is the source of the data. */
  struct st_io_cache    *source_cache;
  uchar                 *buffer;         /* The read buffer. */
  uchar                 *read_end;       /* Behind last valid byte of buffer. */
  int                   running_threads; /* threads not in lock. */
  int                   total_threads;   /* threads sharing the cache. */
  int                   error;           /* Last error. */
} IO_CACHE_SHARE;

typedef struct st_io_cache		/* Used when cacheing files */
{
  /* Offset in file corresponding to the first byte of uchar* buffer. */
  my_off_t pos_in_file;
  /*
    The offset of end of file for READ_CACHE and WRITE_CACHE.
    For SEQ_READ_APPEND it the maximum of the actual end of file and
    the position represented by read_end.
  */
  my_off_t end_of_file;
  /* Points to current read position in the buffer */
  uchar	*read_pos;
  /* the non-inclusive boundary in the buffer for the currently valid read */
  uchar  *read_end;
  uchar  *buffer;				/* The read buffer */
  /* Used in ASYNC_IO */
  uchar  *request_pos;

  /* Only used in WRITE caches and in SEQ_READ_APPEND to buffer writes */
  uchar  *write_buffer;
  /*
    Only used in SEQ_READ_APPEND, and points to the current read position
    in the write buffer. Note that reads in SEQ_READ_APPEND caches can
    happen from both read buffer (uchar* buffer) and write buffer
    (uchar* write_buffer).
  */
  uchar *append_read_pos;
  /* Points to current write position in the write buffer */
  uchar *write_pos;
  /* The non-inclusive boundary of the valid write area */
  uchar *write_end;

  /*
    Current_pos and current_end are convenience variables used by
    my_b_tell() and other routines that need to know the current offset
    current_pos points to &write_pos, and current_end to &write_end in a
    WRITE_CACHE, and &read_pos and &read_end respectively otherwise
  */
  uchar  **current_pos, **current_end;

  /*
    The lock is for append buffer used in SEQ_READ_APPEND cache
    need mutex copying from append buffer to read buffer.
  */
  pthread_mutex_t append_buffer_lock;
  /*
    The following is used when several threads are reading the
    same file in parallel. They are synchronized on disk
    accesses reading the cached part of the file asynchronously.
    It should be set to NULL to disable the feature.  Only
    READ_CACHE mode is supported.
  */
  IO_CACHE_SHARE *share;

  /*
    A caller will use my_b_read() macro to read from the cache
    if the data is already in cache, it will be simply copied with
    memcpy() and internal variables will be accordinging updated with
    no functions invoked. However, if the data is not fully in the cache,
    my_b_read() will call read_function to fetch the data. read_function
    must never be invoked directly.
  */
  int (*read_function)(struct st_io_cache *,uchar *,size_t);
  /*
    Same idea as in the case of read_function, except my_b_write() needs to
    be replaced with my_b_append() for a SEQ_READ_APPEND cache
  */
  int (*write_function)(struct st_io_cache *,const uchar *,size_t);
  /*
    Specifies the type of the cache. Depending on the type of the cache
    certain operations might not be available and yield unpredicatable
    results. Details to be documented later
  */
  enum cache_type type;
  /*
    Callbacks when the actual read I/O happens. These were added and
    are currently used for binary logging of LOAD DATA INFILE - when a
    block is read from the file, we create a block create/append event, and
    when IO_CACHE is closed, we create an end event. These functions could,
    of course be used for other things
  */
  IO_CACHE_CALLBACK pre_read;
  IO_CACHE_CALLBACK post_read;
  IO_CACHE_CALLBACK pre_close;
  /*
    Counts the number of times, when we were forced to use disk. We use it to
    increase the binlog_cache_disk_use and binlog_stmt_cache_disk_use status
    variables.
  */
  ulong disk_writes;
  void* arg;				/* for use by pre/post_read */
  char *file_name;			/* if used with 'open_cached_file' */
  char *dir,*prefix;
  File file; /* file descriptor */
  PSI_file_key file_key; /* instrumented file key */

  /*
    seek_not_done is set by my_b_seek() to inform the upcoming read/write
    operation that a seek needs to be preformed prior to the actual I/O
    error is 0 if the cache operation was successful, -1 if there was a
    "hard" error, and the actual number of I/O-ed bytes if the read/write was
    partial.
  */
  int	seek_not_done,error;
  /* buffer_length is memory size allocated for buffer or write_buffer */
  size_t	buffer_length;
  /* read_length is the same as buffer_length except when we use async io */
  size_t  read_length;
  myf	myflags;			/* Flags used to my_read/my_write */
  /*
    alloced_buffer is 1 if the buffer was allocated by init_io_cache() and
    0 if it was supplied by the user.
    Currently READ_NET is the only one that will use a buffer allocated
    somewhere else
  */
  my_bool alloced_buffer;
} IO_CACHE;

/*
#ifdef MYSQL_CLIENT
class Format_description_log_event;
typedef bool (*read_log_event_filter_function)(char** buf,
                                               ulong*,
                                               const Format_description_log_event*);
#endif
*/

/* Forward declarations */
/*
using binary_log::enum_binlog_checksum_alg;
using binary_log::checksum_crc32;
using binary_log::Log_event_type;
using binary_log::Log_event_header;
using binary_log::Log_event_footer;
using binary_log::Binary_log_event;
using binary_log::Format_description_event;
*/
class Slave_reporting_capability;
class String;
typedef ulonglong sql_mode_t;
typedef struct st_db_worker_hash_entry db_worker_hash_entry;
extern "C"  char server_version[60];
#if defined(HAVE_REPLICATION) && !defined(MYSQL_CLIENT)
int ignored_error_code(int err_code);
#endif
#define PREFIX_SQL_LOAD "SQL_LOAD-"

/**
   Maximum length of the name of a temporary file
   PREFIX LENGTH - 9 
   UUID          - UUID_LENGTH
   SEPARATORS    - 2
   SERVER ID     - 10 (range of server ID 1 to (2^32)-1 = 4,294,967,295)
   FILE ID       - 10 (uint)
   EXTENSION     - 7  (Assuming that the extension is always less than 7 
                       characters)
*/
#define TEMP_FILE_MAX_LEN UUID_LENGTH+38 

/**
   Either assert or return an error.

   In debug build, the condition will be checked, but in non-debug
   builds, the error code given will be returned instead.

   @param COND   Condition to check
   @param ERRNO  Error number to return in non-debug builds
*/
#ifdef DBUG_OFF
#define ASSERT_OR_RETURN_ERROR(COND, ERRNO) \
  do { if (!(COND)) return ERRNO; } while (0)
#else
#define ASSERT_OR_RETURN_ERROR(COND, ERRNO) \
  DBUG_ASSERT(COND)
#endif

#define LOG_READ_EOF    -1
#define LOG_READ_BOGUS  -2
#define LOG_READ_IO     -3
#define LOG_READ_MEM    -5
#define LOG_READ_TRUNC  -6
#define LOG_READ_TOO_LARGE -7
#define LOG_READ_CHECKSUM_FAILURE -8

#define LOG_EVENT_OFFSET 4

#define NUM_LOAD_DELIM_STRS 5

/*****************************************************************************
  sql_ex_info struct
  The strcture contains a refernce to another structure sql_ex_data_info,
  which is defined in binlogevent, and contains the characters specified in
  the sub clause of a LOAD_DATA_INFILE.
  //TODO(WL#7546): Remove this struct and only retain binary_log::sql_ex_data_info
          when the encoder is moved to bapi

 ****************************************************************************/
/*
struct sql_ex_info
{
  sql_ex_info() {}                            
  binary_log::sql_ex_data_info data_info;

  bool write_data(IO_CACHE* file);
  const char* init(const char* buf, const char* buf_end, bool use_new_format);
};
*/

/*****************************************************************************

  MySQL Binary Log

  This log consists of events.  Each event has a fixed-length header,
  possibly followed by a variable length data body.

  The data body consists of an optional fixed length segment (post-header)
  and  an optional variable length segment.

  See the #defines below for the format specifics.

  The events which really update data are Query_log_event,
  Execute_load_query_log_event and old Load_log_event and
  Execute_load_log_event events (Execute_load_query is used together with
  Begin_load_query and Append_block events to replicate LOAD DATA INFILE.
  Create_file/Append_block/Execute_load (which includes Load_log_event)
  were used to replicate LOAD DATA before the 5.0.3).

 ****************************************************************************/

#define MAX_LOG_EVENT_HEADER   ( /* in order of Query_log_event::write */ \
  (LOG_EVENT_HEADER_LEN + /* write_header */ \
  Binary_log_event::QUERY_HEADER_LEN     + /* write_data */   \
  Binary_log_event::EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN) + /*write_post_header_for_derived */ \
  MAX_SIZE_LOG_EVENT_STATUS + /* status */ \
  NAME_LEN + 1)

/*
  The new option is added to handle large packets that are sent from the master 
  to the slave. It is used to increase the thd(max_allowed) for both the
  DUMP thread on the master and the SQL/IO thread on the slave. 
*/
#define MAX_MAX_ALLOWED_PACKET 1024*1024*1024


/* slave event post-header (this event is never written) */

#define SL_MASTER_PORT_OFFSET   8
#define SL_MASTER_POS_OFFSET    0
#define SL_MASTER_HOST_OFFSET   10

/* Intvar event post-header */

/* Intvar event data */
#define I_TYPE_OFFSET        0
#define I_VAL_OFFSET         1


/* 4 bytes which all binlogs should begin with */
#define BINLOG_MAGIC        "\xfe\x62\x69\x6e"

/*
  The 2 flags below were useless :
  - the first one was never set
  - the second one was set in all Rotate events on the master, but not used for
  anything useful.
  So they are now removed and their place may later be reused for other
  flags. Then one must remember that Rotate events in 4.x have
  LOG_EVENT_FORCED_ROTATE_F set, so one should not rely on the value of the
  replacing flag when reading a Rotate event.
  I keep the defines here just to remember what they were.

  #define LOG_EVENT_TIME_F            0x1
  #define LOG_EVENT_FORCED_ROTATE_F   0x2
*/


/**
  @def LOG_EVENT_THREAD_SPECIFIC_F

  If the query depends on the thread (for example: TEMPORARY TABLE).
  Currently this is used by mysqlbinlog to know it must print
  SET @@PSEUDO_THREAD_ID=xx; before the query (it would not hurt to print it
  for every query but this would be slow).
*/
#define LOG_EVENT_THREAD_SPECIFIC_F 0x4

/**
  @def LOG_EVENT_SUPPRESS_USE_F

  Suppress the generation of 'USE' statements before the actual
  statement. This flag should be set for any events that does not need
  the current database set to function correctly. Most notable cases
  are 'CREATE DATABASE' and 'DROP DATABASE'.

  This flags should only be used in exceptional circumstances, since
  it introduce a significant change in behaviour regarding the
  replication logic together with the flags --binlog-do-db and
  --replicated-do-db.
 */
#define LOG_EVENT_SUPPRESS_USE_F    0x8

/*
  Note: this is a place holder for the flag
  LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F (0x10), which is not used any
  more, please do not reused this value for other flags.
 */

/**
   @def LOG_EVENT_ARTIFICIAL_F
   
   Artificial events are created arbitarily and not written to binary
   log

   These events should not update the master log position when slave
   SQL thread executes them.
*/
#define LOG_EVENT_ARTIFICIAL_F 0x20

/**
   @def LOG_EVENT_RELAY_LOG_F
   
   Events with this flag set are created by slave IO thread and written
   to relay log
*/
#define LOG_EVENT_RELAY_LOG_F 0x40

/**
   @def LOG_EVENT_IGNORABLE_F

   For an event, 'e', carrying a type code, that a slave,
   's', does not recognize, 's' will check 'e' for
   LOG_EVENT_IGNORABLE_F, and if the flag is set, then 'e'
   is ignored. Otherwise, 's' acknowledges that it has
   found an unknown event in the relay log.
*/
#define LOG_EVENT_IGNORABLE_F 0x80

/**
   @def LOG_EVENT_NO_FILTER_F

   Events with this flag are not filtered (e.g. on the current
   database) and are always written to the binary log regardless of
   filters.
*/
#define LOG_EVENT_NO_FILTER_F 0x100

/**
   MTS: group of events can be marked to force its execution
   in isolation from any other Workers.
   So it's a marker for Coordinator to memorize and perform necessary
   operations in order to guarantee no interference from other Workers.
   The flag can be set ON only for an event that terminates its group.
   Typically that is done for a transaction that contains 
   a query accessing more than OVER_MAX_DBS_IN_EVENT_MTS databases.
*/
#define LOG_EVENT_MTS_ISOLATE_F 0x200


/**
  @def OPTIONS_WRITTEN_TO_BIN_LOG

  OPTIONS_WRITTEN_TO_BIN_LOG are the bits of thd->options which must
  be written to the binlog. OPTIONS_WRITTEN_TO_BIN_LOG could be
  written into the Format_description_log_event, so that if later we
  don't want to replicate a variable we did replicate, or the
  contrary, it's doable. But it should not be too hard to decide once
  for all of what we replicate and what we don't, among the fixed 32
  bits of thd->options.

  I (Guilhem) have read through every option's usage, and it looks
  like OPTION_AUTO_IS_NULL and OPTION_NO_FOREIGN_KEYS are the only
  ones which alter how the query modifies the table. It's good to
  replicate OPTION_RELAXED_UNIQUE_CHECKS too because otherwise, the
  slave may insert data slower than the master, in InnoDB.
  OPTION_BIG_SELECTS is not needed (the slave thread runs with
  max_join_size=HA_POS_ERROR) and OPTION_BIG_TABLES is not needed
  either, as the manual says (because a too big in-memory temp table
  is automatically written to disk).
*/
#define OPTIONS_WRITTEN_TO_BIN_LOG \
  (OPTION_AUTO_IS_NULL | OPTION_NO_FOREIGN_KEY_CHECKS |  \
   OPTION_RELAXED_UNIQUE_CHECKS | OPTION_NOT_AUTOCOMMIT)

/* Shouldn't be defined before */
#define EXPECTED_OPTIONS \
  ((1ULL << 14) | (1ULL << 26) | (1ULL << 27) | (1ULL << 19))


#undef EXPECTED_OPTIONS         /* You shouldn't use this one */

/**
   Maximum value of binlog logical timestamp.
*/
const int64 SEQ_MAX_TIMESTAMP= LLONG_MAX;

 inline uint32 uint4korr(const uchar *A) { return *((uint32*) A); }
 inline uint32 uint4korr(const char *pT)
{
  return uint4korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}
#ifdef MYSQL_SERVER
class String;
class MYSQL_BIN_LOG;
class THD;
#endif

class Format_description_log_event;
class Relay_log_info;
class Slave_worker;
class Slave_committed_queue;


enum enum_base64_output_mode {
  BASE64_OUTPUT_NEVER= 0,
  BASE64_OUTPUT_AUTO= 1,
  BASE64_OUTPUT_UNSPEC= 2,
  BASE64_OUTPUT_DECODE_ROWS= 3,
  /* insert new output modes here */
  BASE64_OUTPUT_MODE_COUNT
};


/*
  A specific to the database-scheduled MTS type.
*/
typedef struct st_mts_db_names
{
  const char *name[MAX_DBS_IN_EVENT_MTS];
  int  num;
} Mts_db_names;

/**
  @class Log_event

  This is the abstract base class for binary log events.
  
  @section Log_event_binary_format Binary Format

  The format of the event is described @ref Binary_log_event_format "here".

  @subsection Log_event_format_of_atomic_primitives Format of Atomic Primitives

  - All numbers, whether they are 16-, 24-, 32-, or 64-bit numbers,
  are stored in little endian, i.e., the least significant byte first,
  unless otherwise specified.

*/
class Log_event
{
public:
  /**
     Enumeration of what kinds of skipping (and non-skipping) that can
     occur when the slave executes an event.

     @see shall_skip
     @see do_shall_skip
   */
  enum enum_skip_reason {
    /**
       Don't skip event.
    */
    EVENT_SKIP_NOT,

    /**
       Skip event by ignoring it.

       This means that the slave skip counter will not be changed.
    */
    EVENT_SKIP_IGNORE,

    /**
       Skip event and decrease skip counter.
    */
    EVENT_SKIP_COUNT
  };

protected:
  enum enum_event_cache_type 
  {
    EVENT_INVALID_CACHE= 0,
    /* 
      If possible the event should use a non-transactional cache before
      being flushed to the binary log. This means that it must be flushed
      right after its correspondent statement is completed.
    */
    EVENT_STMT_CACHE,
    /* 
      The event should use a transactional cache before being flushed to
      the binary log. This means that it must be flushed upon commit or 
      rollback. 
    */
    EVENT_TRANSACTIONAL_CACHE,
    /* 
      The event must be written directly to the binary log without going
      through any cache.
    */
    EVENT_NO_CACHE,
    /*
       If there is a need for different types, introduce them before this.
    */
    EVENT_CACHE_COUNT
  };

  enum enum_event_logging_type
  {
    EVENT_INVALID_LOGGING= 0,
    /*
      The event must be written to a cache and upon commit or rollback
      written to the binary log.
    */
    EVENT_NORMAL_LOGGING,
    /*
      The event must be written to an empty cache and immediatly written
      to the binary log without waiting for any other event.
    */
    EVENT_IMMEDIATE_LOGGING,
    /*
       If there is a need for different types, introduce them before this.
    */
    EVENT_CACHE_LOGGING_COUNT
  };

  bool is_valid_param;
  /**
    Writes the common header of this event to the given memory buffer.

    This does not update the checksum.

    @note This has the following form:

    +---------+---------+---------+------------+-----------+-------+
    |timestamp|type code|server_id|event_length|end_log_pos|flags  |
    |4 bytes  |1 byte   |4 bytes  |4 bytes     |4 bytes    |2 bytes|
    +---------+---------+---------+------------+-----------+-------+

    @param buf Memory buffer to write to. This must be at least
    LOG_EVENT_HEADER_LEN bytes long.

    @return The number of bytes written, i.e., always
    LOG_EVENT_HEADER_LEN.
  */
  uint32 write_header_to_memory(uchar *buf);
  /**
    Writes the common-header of this event to the given IO_CACHE and
    updates the checksum.

    @param file The event will be written to this IO_CACHE.

    @param data_length The length of the post-header section plus the
    length of the data section; i.e., the length of the event minus
    the common-header and the checksum.
  */
  bool write_header(IO_CACHE* file, size_t data_length);
  //bool write_footer(IO_CACHE* file);
  //my_bool need_checksum();


public:
  /*
     A temp buffer for read_log_event; it is later analysed according to the
     event's type, and its content is distributed in the event-specific fields.
  */
  char *temp_buf;
  /* The number of seconds the query took to run on the master. */
  ulong exec_time;

  /*
    The master's server id (is preserved in the relay log; used to
    prevent from infinite loops in circular replication).
  */
  uint32 server_id;

  
  /**
    A storage to cache the global system variable's value.
    Handling of a separate event will be governed its member.
  */
  ulong rbr_exec_mode;

  /**
    Defines the type of the cache, if any, where the event will be
    stored before being flushed to disk.
  */
  enum_event_cache_type event_cache_type;

  /**
    Defines when information, i.e. event or cache, will be flushed
    to disk.
  */
  enum_event_logging_type event_logging_type;
  /**
    Placeholder for event checksum while writing to binlog.
  */
  ha_checksum crc;
  /**
    Index in @c rli->gaq array to indicate a group that this event is
    purging. The index is set by Coordinator to a group terminator
    event is checked by Worker at the event execution. The indexed
    data represent the Worker progress status.
  */
  ulong mts_group_idx;

  /**
   The Log_event_header class contains the variable present
   in the common header
  */
  binary_log::Log_event_header *common_header;

  /**
   The Log_event_footer class contains the variable present
   in the common footer. Currently, footer contains only the checksum_alg.
  */
  binary_log::Log_event_footer *common_footer;
  /**
    MTS: associating the event with either an assigned Worker or Coordinator.
    Additionally the member serves to tag deferred (IRU) events to avoid
    the event regular time destruction.
  */
  Relay_log_info *worker;

  /** 
    A copy of the main rli value stored into event to pass to MTS worker rli
  */
  ulonglong future_event_relay_log_pos;

#ifdef MYSQL_SERVER
  THD* thd;
  /**
     Partition info associate with event to deliver to MTS event applier
  */
  db_worker_hash_entry *mts_assigned_partitions[MAX_DBS_IN_EVENT_MTS];
/*
  Log_event(Log_event_header *header, Log_event_footer *footer,
            enum_event_cache_type cache_type_arg,
            enum_event_logging_type logging_type_arg);
            
  Log_event( uint16 flags_arg,
            enum_event_cache_type cache_type_arg,
            enum_event_logging_type logging_type_arg,
            Log_event_header *header, Log_event_footer *footer); */
  /*
    read_log_event() functions read an event from a binlog or relay
    log; used by SHOW BINLOG EVENTS, the binlog_dump thread on the
    master (reads master's binlog), the slave IO thread (reads the
    event sent by binlog_dump), the slave SQL thread (reads the event
    from the relay log).  If mutex is 0, the read will proceed without
    mutex.  We need the description_event to be able to parse the
    event (to know the post-header's size); in fact in read_log_event
    we detect the event's type, then call the specific event's
    constructor and pass description_event as an argument.
  */
  static Log_event* read_log_event(IO_CACHE* file,
                                   pthread_mutex_t* log_lock,
                                   const Format_description_log_event
                                   *description_event,
                                   my_bool crc_check);


  /*
   This function will read the common header into the buffer.

   @param[in]         log_cache The IO_CACHE to read from.
   @param[in/out]     header The buffer where to read the common header. This
                      buffer must be at least LOG_EVENT_MINIMAL_HEADER_LEN long.

   @returns           false on success, true otherwise.
  */
  inline static bool peek_event_header(char *header, IO_CACHE *log_cache)
  {
    DBUG_ENTER("Log_event::peek_event_header");
    if (my_b_read(log_cache, (uchar*) header, LOG_EVENT_MINIMAL_HEADER_LEN))
      DBUG_RETURN(true);
    DBUG_RETURN(false);
  }

  /*
   This static function will read the event length from the common
   header that is on the IO_CACHE. Note that the IO_CACHE read position
   will not be updated.

   @param[in]         log_cache The IO_CACHE to read from.
   @param[out]        length A pointer to the memory position where to store
                      the length value.
   @param[out]        header_buffer An optional pointer to a buffer to store
                      the event header.

   @returns           false on success, true otherwise.
  */

  inline static bool peek_event_length(uint32* length, IO_CACHE *log_cache,
                                       char *header_buffer)
  {
    DBUG_ENTER("Log_event::peek_event_length");
    char local_header_buffer[LOG_EVENT_MINIMAL_HEADER_LEN];
    char *header= header_buffer != NULL ? header_buffer : local_header_buffer;
    if (peek_event_header(header, log_cache))
      DBUG_RETURN(true);
    *length= uint4korr(header + EVENT_LEN_OFFSET);
    DBUG_RETURN(false);
  }

  /**
    Reads an event from a binlog or relay log. Used by the dump thread
    this method reads the event into a raw buffer without parsing it.

    @Note If mutex is 0, the read will proceed without mutex.

    @Note If a log name is given than the method will check if the
    given binlog is still active.

    @param[in]  file                log file to be read
    @param[out] packet              packet to hold the event
    @param[in]  lock                the lock to be used upon read
    @param[in]  checksum_alg_arg    the checksum algorithm
    @param[in]  log_file_name_arg   the log's file name
    @param[out] is_binlog_active    is the current log still active
    @param[in]  event_header        the actual event header. Passing this
                                    parameter will make the function to skip
                                    reading the event header.

    @retval 0                   success
    @retval LOG_READ_EOF        end of file, nothing was read
    @retval LOG_READ_BOGUS      malformed event
    @retval LOG_READ_IO         io error while reading
    @retval LOG_READ_MEM        packet memory allocation failed
    @retval LOG_READ_TRUNC      only a partial event could be read
    @retval LOG_READ_TOO_LARGE  event too large
   */
  static int read_log_event(IO_CACHE* file, String* packet,
                            mysql_mutex_t* log_lock,
                            binary_log::enum_binlog_checksum_alg checksum_alg_arg,
                            const char *log_file_name_arg= NULL,
                            bool* is_binlog_active= NULL,
                            char *event_header= NULL);

  /*
    init_show_field_list() prepares the column names and types for the
    output of SHOW BINLOG EVENTS; it is used only by SHOW BINLOG
    EVENTS.
  */
  static void init_show_field_list(List<Item>* field_list);
//#ifdef HAVE_REPLICATION
  //int net_send(Protocol *protocol, const char* log_name, my_off_t pos);

  /**
    Stores a string representation of this event in the Protocol.
    This is used by SHOW BINLOG EVENTS.

    @retval 0 success
    @retval nonzero error
  */
  //virtual int pack_info(Protocol *protocol);

//#endif /* HAVE_REPLICATION */
  virtual const char* get_db()
  {
    return thd ? thd->db().str : NULL;
  }
#else // ifdef MYSQL_SERVER
/*
  Log_event(Log_event_header *header, Log_event_footer *footer,
            enum_event_cache_type cache_type_arg= EVENT_INVALID_CACHE,
            enum_event_logging_type logging_type_arg= EVENT_INVALID_LOGGING)
  : temp_buf(0),  event_cache_type(cache_type_arg),
    event_logging_type(logging_type_arg), is_valid(false)
  {
    common_header= header;
    common_footer= footer;
  }
  */
    /* avoid having to link mysqlbinlog against libpthread 
  static Log_event* read_log_event(IO_CACHE* file,
                                   const Format_description_log_event
                                   *description_event, my_bool crc_check,
                                   read_log_event_filter_function f);*/
  /* print*() functions are used by mysqlbinlog */

#endif // ifdef MYSQL_SERVER ... else

  void *operator new(size_t size);

  static void operator delete(void *ptr, size_t)
  {
    free(ptr);
  }

  /* Placement version of the above operators */
  static void *operator new(size_t, void* ptr) { return ptr; }
  static void operator delete(void*, void*) { }
  /**
    Write the given buffer to the given IO_CACHE, updating the
    checksum if checksums are enabled.

    @param file The IO_CACHE to write to.
    @param buf The buffer to write.
    @param data_length The number of bytes to write.

    @retval false Success.
    @retval true Error.
  */
  //bool wrapper_my_b_safe_write(IO_CACHE* file, const uchar* buf, size_t data_length);

#ifdef MYSQL_SERVER
  virtual bool write(IO_CACHE* file)
  {
    return(write_header(file, get_data_size()) ||
	   write_data_header(file) ||
	   write_data_body(file) ||
	   write_footer(file));
  }
  virtual bool write_data_header(IO_CACHE* file)
  { return 0; }
  virtual bool write_data_body(IO_CACHE* file MY_ATTRIBUTE((unused)))
  { return 0; }
  inline time_t get_time()
  {
    /* Not previously initialized */
    if (!common_header->when.tv_sec && !common_header->when.tv_usec)
    {
      THD *tmp_thd= thd ? thd : current_thd;
      if (tmp_thd)
        common_header->when= tmp_thd->start_time;
      else
        my_micro_time_to_timeval(my_micro_time(), &(common_header->when));
    }
    return (time_t) common_header->when.tv_sec;
  }
#endif
  Log_event_type get_type_code()
  {
    return common_header->type_code;
  }

  /**
    Return true if the event has to be logged using SBR for DMLs.
  */
  virtual bool is_sbr_logging_format() const
  {
    return false;
  }
  /**
    Return true if the event has to be logged using RBR for DMLs.
  */
  virtual bool is_rbr_logging_format() const
  {
    return false;
  }

  /*
   is_valid is event specific sanity checks to determine that the
    object is correctly initialized.
  */
  bool is_valid() { return is_valid_param; }
  void set_artificial_event()
  {
    common_header->flags |= LOG_EVENT_ARTIFICIAL_F;
  }
  void set_relay_log_event()
  {
    common_header->flags |= LOG_EVENT_RELAY_LOG_F;
  }
  bool is_artificial_event() const
  {
    return common_header->flags & LOG_EVENT_ARTIFICIAL_F;
  }
  bool is_relay_log_event() const
  {
    return common_header->flags & LOG_EVENT_RELAY_LOG_F;
  }
  bool is_ignorable_event() const
  {
    return common_header->flags & LOG_EVENT_IGNORABLE_F;
  }
  bool is_no_filter_event() const
  {
    return common_header->flags & LOG_EVENT_NO_FILTER_F;
  }
  inline bool is_using_trans_cache() const
  {
    return (event_cache_type == EVENT_TRANSACTIONAL_CACHE);
  }
  inline bool is_using_stmt_cache() const
  {
    return(event_cache_type == EVENT_STMT_CACHE);
  }
  inline bool is_using_immediate_logging() const
  {
    return(event_logging_type == EVENT_IMMEDIATE_LOGGING);
  }

  /*
     For the events being decoded in BAPI, common_header should
     point to the header object which is contained within the class
     Binary_log_event.
  */
  Log_event(Log_event_header *header,
            Log_event_footer *footer);

  virtual ~Log_event() { free_temp_buf(); }
  void register_temp_buf(char* buf) { temp_buf = buf; }
  void free_temp_buf()
  {
    if (temp_buf)
    {
      my_free(temp_buf);
      temp_buf = 0;
    }
  }
  /*
    Get event length for simple events. For complicated events the length
    is calculated during write()
  */
  virtual size_t get_data_size() { return 0;}
  static Log_event* read_log_event(const char* buf, uint event_len,
				   const char **error,
                                   const Format_description_log_event
                                   *description_event, my_bool crc_check);
  /**
    Returns the human readable name of the given event type.
  */
  static const char* get_type_str(Log_event_type type);
  /**
    Returns the human readable name of this event's type.
  */
  const char* get_type_str();
  /* Return start of query time or current time */

#if defined(MYSQL_SERVER) && defined(HAVE_REPLICATION)
  /**
     Is called from get_mts_execution_mode() to

     @param  is_scheduler_dbname
                   The current scheduler type.
                   In case the db-name scheduler certain events
                   can't be applied in parallel.

     @return TRUE  if the event needs applying with synchronization
                   agaist Workers, otherwise
             FALSE

     @note There are incompatile combinations such as referred further events
           are wrapped with BEGIN/COMMIT. Such cases should be identified
           by the caller and treats correspondingly.

           todo: to mts-support Old master Load-data related events
  */
  bool is_mts_sequential_exec(bool is_scheduler_dbname)
  {
    return
      ((get_type_code() == binary_log::LOAD_EVENT         ||
        get_type_code() == binary_log::CREATE_FILE_EVENT  ||
        get_type_code() == binary_log::NEW_LOAD_EVENT     ||
        get_type_code() == binary_log::EXEC_LOAD_EVENT)    &&
       is_scheduler_dbname)                                  ||
      get_type_code() == binary_log::START_EVENT_V3          ||
      get_type_code() == binary_log::STOP_EVENT              ||
      get_type_code() == binary_log::ROTATE_EVENT            ||
      get_type_code() == binary_log::SLAVE_EVENT             ||
      get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT||
      get_type_code() == binary_log::INCIDENT_EVENT;
  }

private:

  /*
    possible decisions by get_mts_execution_mode().
    The execution mode can be PARALLEL or not (thereby sequential
    unless impossible at all). When it's sequential it further  breaks into
    ASYNChronous and SYNChronous.
  */
  enum enum_mts_event_exec_mode
  {
    /*
      Event is run by a Worker.
    */
    EVENT_EXEC_PARALLEL,
    /*
      Event is run by Coordinator.
    */
    EVENT_EXEC_ASYNC,
    /*
      Event is run by Coordinator and requires synchronization with Workers.
    */
    EVENT_EXEC_SYNC,
    /*
      Event can't be executed neither by Workers nor Coordinator.
    */
    EVENT_EXEC_CAN_NOT
  };

  /**
     MTS Coordinator finds out a way how to execute the current event.

     Besides the parallelizable case, some events have to be applied by
     Coordinator concurrently with Workers and some to require synchronization
     with Workers (@c see wait_for_workers_to_finish) before to apply them.

     @param slave_server_id   id of the server, extracted from event
     @param mts_in_group      the being group parsing status, true
                              means inside the group
     @param  is_scheduler_dbname
                              true when the current submode (scheduler)
                              is of DB_NAME type.

     @retval EVENT_EXEC_PARALLEL  if event is executed by a Worker
     @retval EVENT_EXEC_ASYNC     if event is executed by Coordinator
     @retval EVENT_EXEC_SYNC      if event is executed by Coordinator
                                  with synchronization against the Workers
  */
  enum enum_mts_event_exec_mode get_mts_execution_mode(ulong slave_server_id,
                                                       bool mts_in_group,
                                                       bool is_dbname_type)
  {
    /*
      Slave workers are unable to handle Format_description_log_event,
      Rotate_log_event and Previous_gtids_log_event correctly.
      However, when a transaction spans multiple relay logs, these
      events occur in the middle of a transaction. The way we handle
      this is by marking the events as 'ASYNC', meaning that the
      coordinator thread will handle the events without stopping the
      worker threads.

      @todo Refactor this: make Log_event::get_slave_worker handle
      transaction boundaries in a more robust way, so that it is able
      to process Format_description_log_event, Rotate_log_event, and
      Previous_gtids_log_event.  Then, when these events occur in the
      middle of a transaction, make them part of the transaction so
      that the worker that handles the transaction handles these
      events too. /Sven
    */
    if (
        /*
          When a Format_description_log_event occurs in the middle of
          a transaction, it either has the slave's server_id, or has
          end_log_pos==0.

          @todo This does not work when master and slave have the same
          server_id and replicate-same-server-id is enabled, since
          events that are not in the middle of a transaction will be
          executed in ASYNC mode in that case.
        */
        (get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT &&
         ((server_id == (uint32) ::server_id) || (common_header->log_pos == 0))) ||
        /*
          All Previous_gtids_log_events in the relay log are generated
          by the slave. They don't have any meaning to the applier, so
          they can always be ignored by the applier. So we can process
          them asynchronously by the coordinator. It is also important
          to not feed them to workers because that confuses
          get_slave_worker.
        */
        (get_type_code() == binary_log::PREVIOUS_GTIDS_LOG_EVENT) ||
        /*
          Rotate_log_event can occur in the middle of a transaction.
          When this happens, either it is a Rotate event generated on
          the slave which has the slave's server_id, or it is a Rotate
          event that originates from a master but has end_log_pos==0.
        */
        (get_type_code() == binary_log::ROTATE_EVENT &&
         ((server_id == (uint32) ::server_id) ||
          (common_header->log_pos == 0 && mts_in_group))))
      return EVENT_EXEC_ASYNC;
    else if (is_mts_sequential_exec(is_dbname_type))
      return EVENT_EXEC_SYNC;
    else
      return EVENT_EXEC_PARALLEL;
  }

  /**
     @return index  in \in [0, M] range to indicate
             to be assigned worker;
             M is the max index of the worker pool.
  */
  Slave_worker *get_slave_worker(Relay_log_info *rli);

  /*
    Group of events can be marked to force its execution
    in isolation from any other Workers.
    Typically that is done for a transaction that contains
    a query accessing more than OVER_MAX_DBS_IN_EVENT_MTS databases.
    Factually that's a sequential mode where a Worker remains to
    be the applier.
  */
  virtual void set_mts_isolate_group()
  {
    DBUG_ASSERT(ends_group() ||
                get_type_code() == binary_log::QUERY_EVENT ||
                get_type_code() == binary_log::EXEC_LOAD_EVENT ||
                get_type_code() == binary_log::EXECUTE_LOAD_QUERY_EVENT);
    common_header->flags |= LOG_EVENT_MTS_ISOLATE_F;
  }


public:
  /**
     The method fills in pointers to event's database name c-strings
     to a supplied array.
     In other than Query-log-event case the returned array contains
     just one item.
     @param[out] arg pointer to a struct containing char* array
                     pointers to be filled in and the number
                     of filled instances.

     @return     number of the filled intances indicating how many
                 databases the event accesses.
  */
  virtual uint8 get_mts_dbs(Mts_db_names *arg)
  {
    arg->name[0]= get_db();

    return arg->num= mts_number_dbs();
  }


  /**
     @return TRUE  if events carries partitioning data (database names).
  */
  bool contains_partition_info(bool);

  /*
    @return  the number of updated by the event databases.

    @note In other than Query-log-event case that's one.
  */
  virtual uint8 mts_number_dbs() { return 1; }

  /**
    @return TRUE  if the terminal event of a group is marked to
                  execute in isolation from other Workers,
            FASE  otherwise
  */
  bool is_mts_group_isolated() { return common_header->flags &
                                        LOG_EVENT_MTS_ISOLATE_F; }

  /**
     Events of a certain type can start or end a group of events treated
     transactionally wrt binlog.

     Public access is required by implementation of recovery + skip.

     @return TRUE  if the event starts a group (transaction)
             FASE  otherwise
  */
  virtual bool starts_group() { return false; }

  /**
     @return TRUE  if the event ends a group (transaction)
             FASE  otherwise
  */
  virtual bool ends_group()   { return false; }

  /**
     Apply the event to the database.

     This function represents the public interface for applying an
     event.

     @see do_apply_event
   */
  int apply_event(Relay_log_info *rli);

  /**
     Apply the GTID event in curr_group_data to the database.

     @param rli Pointer to coordinato's relay log info.

     @retval 0 success
     @retval 1 error
  */
  inline int apply_gtid_event(Relay_log_info *rli);

  /**
     Update the relay log position.

     This function represents the public interface for "stepping over"
     the event and will update the relay log information.

     @see do_update_pos
   */
  int update_pos(Relay_log_info *rli)
  {
    return do_update_pos(rli);
  }

  /**
     Decide if the event shall be skipped, and the reason for skipping
     it.

     @see do_shall_skip
   */
  enum_skip_reason shall_skip(Relay_log_info *rli)
  {
    DBUG_ENTER("Log_event::shall_skip");
    enum_skip_reason ret= do_shall_skip(rli);
    DBUG_PRINT("info", ("skip reason=%d=%s", ret,
                        ret==EVENT_SKIP_NOT ? "NOT" :
                        ret==EVENT_SKIP_IGNORE ? "IGNORE" : "COUNT"));
    DBUG_RETURN(ret);
  }

  /**
    Primitive to apply an event to the database.

    This is where the change to the database is made.

    @note The primitive is protected instead of private, since there
    is a hierarchy of actions to be performed in some cases.

    @see Format_description_log_event::do_apply_event()

    @param rli Pointer to relay log info structure

    @retval 0     Event applied successfully
    @retval errno Error code if event application failed
  */
  virtual int do_apply_event(Relay_log_info const *rli)
  {
    return 0;                /* Default implementation does nothing */
  }

  //virtual int do_apply_event_worker(Slave_worker *w);

protected:

  /**
     Helper function to ignore an event w.r.t. the slave skip counter.

     This function can be used inside do_shall_skip() for functions
     that cannot end a group. If the slave skip counter is 1 when
     seeing such an event, the event shall be ignored, the counter
     left intact, and processing continue with the next event.

     A typical usage is:
     @code
     enum_skip_reason do_shall_skip(Relay_log_info *rli) {
       return continue_group(rli);
     }
     @endcode

     @return Skip reason
   */
  enum_skip_reason continue_group(Relay_log_info *rli);

  /**
     Advance relay log coordinates.

     This function is called to advance the relay log coordinates to
     just after the event.  It is essential that both the relay log
     coordinate and the group log position is updated correctly, since
     this function is used also for skipping events.

     Normally, each implementation of do_update_pos() shall:

     - Update the event position to refer to the position just after
       the event.

     - Update the group log position to refer to the position just
       after the event <em>if the event is last in a group</em>

     @param rli Pointer to relay log info structure

     @retval 0     Coordinates changed successfully
     @retval errno Error code if advancing failed (usually just
                   1). Observe that handler errors are returned by the
                   do_apply_event() function, and not by this one.
   */
  //virtual int do_update_pos(Relay_log_info *rli);


  /**
     Decide if this event shall be skipped or not and the reason for
     skipping it.

     The default implementation decide that the event shall be skipped
     if either:

     - the server id of the event is the same as the server id of the
       server and <code>rli->replicate_same_server_id</code> is true,
       or

     - if <code>rli->slave_skip_counter</code> is greater than zero.

     @see do_apply_event
     @see do_update_pos

     @retval Log_event::EVENT_SKIP_NOT
     The event shall not be skipped and should be applied.

     @retval Log_event::EVENT_SKIP_IGNORE
     The event shall be skipped by just ignoring it, i.e., the slave
     skip counter shall not be changed. This happends if, for example,
     the originating server id of the event is the same as the server
     id of the slave.

     @retval Log_event::EVENT_SKIP_COUNT
     The event shall be skipped because the slave skip counter was
     non-zero. The caller shall decrease the counter by one.
   */
  //virtual enum_skip_reason do_shall_skip(Relay_log_info *rli);
#endif
};


/*
   One class for each type of event.
   Two constructors for each class:
   - one to create the event for logging (when the server acts as a master),
   called after an update to the database is done,
   which accepts parameters like the query, the database, the options for LOAD
   DATA INFILE...
   - one to create the event from a packet (when the server acts as a slave),
   called before reproducing the update, which accepts parameters (like a
   buffer). Used to read from the master, from the relay log, and in
   mysqlbinlog. This constructor must be format-tolerant.
*/

/**
  A @Query event is written to the binary log whenever the database is
  modified on the master, unless row based logging is used.

  Query_log_event is created for logging, and is called after an update to the
  database is done. It is used when the server acts as the master.

  Virtual inheritance is required here to handle the diamond problem in
  the class Execute_load_query_log_event.
  The diamond structure is explained in @Excecute_load_query_log_event

  @internal
  The inheritance structure is as follows:

            Binary_log_event
                   ^
                   |
                   |
            Query_event  Log_event
                   \       /
         <<virtual>>\     /
                     \   /
                Query_log_event
  @endinternal
*/
class Query_log_event: public virtual binary_log::Query_event, public Log_event
{
protected:
  Log_event_header::Byte* data_buf;
public:

  /*
    For events created by Query_log_event::do_apply_event (and
    Load_log_event::do_apply_event()) we need the *original* thread
    id, to be able to log the event with the original (=master's)
    thread id (fix for BUG#1686).
  */
  pthread_t slave_proxy_id;

#ifdef MYSQL_SERVER
/*
  Query_log_event(THD* thd_arg, const char* query_arg, size_t query_length,
                  bool using_trans, bool immediate, bool suppress_use,
                  int error, bool ignore_command= FALSE);
                  */
  const char* get_db() { return db; }

  /**
     @param[out] arg pointer to a struct containing char* array
                     pointers be filled in and the number of
                     filled instances.
                     In case the number exceeds MAX_DBS_IN_EVENT_MTS,
                     the overfill is indicated with assigning the number to
                     OVER_MAX_DBS_IN_EVENT_MTS.

     @return     number of databases in the array or OVER_MAX_DBS_IN_EVENT_MTS.
  */
  virtual uint8 get_mts_dbs(Mts_db_names* arg)
  {
    if (mts_accessed_dbs == OVER_MAX_DBS_IN_EVENT_MTS)
    {
      // the empty string db name is special to indicate sequential applying
      mts_accessed_db_names[0][0]= 0;
    }
    else
    {
      for (uchar i= 0; i < mts_accessed_dbs; i++)
      {
        char *db_name= mts_accessed_db_names[i];

        // Only default database is rewritten.
        if (!rpl_filter->is_rewrite_empty() && !strcmp(get_db(), db_name))
        {
          size_t dummy_len;
          const char *db_filtered= rpl_filter->get_rewrite_db(db_name, &dummy_len);
          // db_name != db_filtered means that db_name is rewritten.
          if (strcmp(db_name, db_filtered))
            db_name= (char*)db_filtered;
        }
        arg->name[i]= db_name;
      }
    }
    return arg->num= mts_accessed_dbs;
  }

  void attach_temp_tables_worker(THD*, const Relay_log_info *);
  void detach_temp_tables_worker(THD*, const Relay_log_info *);

  virtual uchar mts_number_dbs() { return mts_accessed_dbs; }


#else
  //static bool rewrite_db_in_buffer(char **buf, ulong *event_len,
   //                                const Format_description_log_event *fde);
#endif

  Query_log_event();

  Query_log_event(const char* buf, uint event_len,
                  const Format_description_event *description_event,
                  Log_event_type event_type);
  ~Query_log_event()
  {
    if (data_buf)
      my_free(data_buf);
  }

  bool write(IO_CACHE* file);
  virtual bool write_post_header_for_derived(IO_CACHE* file) { return false; }


  /*
    Returns number of bytes additionally written to post header by derived
    events (so far it is only Execute_load_query event).
  */
  virtual ulong get_post_header_size_for_derived() { return 0; }
  /* Writes derived event-specific part of post header. */

public:        /* !!! Public in this patch to allow old usage */
#if defined(MYSQL_SERVER) && defined(HAVE_REPLICATION)
  virtual enum_skip_reason do_shall_skip(Relay_log_info *rli);
  virtual int do_apply_event(Relay_log_info const *rli);
  virtual int do_update_pos(Relay_log_info *rli);

  int do_apply_event(Relay_log_info const *rli,
                     const char *query_arg,
                     size_t q_len_arg);
#endif /* HAVE_REPLICATION */
  /*
    If true, the event always be applied by slave SQL thread or be printed by
    mysqlbinlog
   */
  bool is_trans_keyword() const
  {
    /*
      Before the patch for bug#50407, The 'SAVEPOINT and ROLLBACK TO'
      queries input by user was written into log events directly.
      So the keywords can be written in both upper case and lower case
      together, strncasecmp is used to check both cases. they also could be
      binlogged with comments in the front of these keywords. for examples:
        / * bla bla * / SAVEPOINT a;
        / * bla bla * / ROLLBACK TO a;
      but we don't handle these cases and after the patch, both quiries are
      binlogged in upper case with no comments.
     */
    return !strncmp(query, "BEGIN", q_len) ||
      !strncmp(query, "COMMIT", q_len) ||
      !native_strncasecmp(query, "SAVEPOINT", 9) ||
      !native_strncasecmp(query, "ROLLBACK", 8) ||
      !native_strncasecmp(query, STRING_WITH_LEN("XA START")) ||
      !native_strncasecmp(query, STRING_WITH_LEN("XA END")) ||
      !native_strncasecmp(query, STRING_WITH_LEN("XA PREPARE")) ||
      !native_strncasecmp(query, STRING_WITH_LEN("XA COMMIT")) ||
      !native_strncasecmp(query, STRING_WITH_LEN("XA ROLLBACK"));
  }

  /**
    When a query log event contains a non-transaction control statement, we
    assume that it is changing database content (DML) and was logged using
    binlog_format=statement.

    @return True the event represents a statement that was logged using SBR
            that can change database content.
            False for transaction control statements.
  */
  bool is_sbr_logging_format() const
  {
    return !is_trans_keyword();
  }

  /**
     Notice, DDL queries are logged without BEGIN/COMMIT parentheses
     and identification of such single-query group
     occures within logics of @c get_slave_worker().
  */

  bool starts_group()
  {
    return
      !strncmp(query, "BEGIN", q_len) ||
      !strncmp(query, STRING_WITH_LEN("XA START"));
  }

  virtual bool ends_group()
  {
    return
      !strncmp(query, "COMMIT", q_len) ||
      (!native_strncasecmp(query, STRING_WITH_LEN("ROLLBACK"))
       && native_strncasecmp(query, STRING_WITH_LEN("ROLLBACK TO "))) ||
      !strncmp(query, STRING_WITH_LEN("XA ROLLBACK"));
  }
  static size_t get_query(const char *buf, size_t length,
                          const Format_description_log_event *fd_event,
                          char** query);

  bool is_query_prefix_match(const char* pattern, uint p_len)
  {
    return !strncmp(query, pattern, p_len);
  }
};



/**
  @class Start_log_event_v3

  Start_log_event_v3 is the Start_log_event of binlog format 3 (MySQL 3.23 and
  4.x).

  @internal
  The inheritance structure in the current design for the classes is
  as follows:
                  Binary_log_event
                        ^
                        |
                        |
                Start_event_v3   Log_event
                         \       /
               <<virtual>>\     /
                           \   /
                       Start_log_event_v3
  @endinternal
*/
class Start_log_event_v3: public virtual binary_log::Start_event_v3, public Log_event
{
public:
#ifdef MYSQL_SERVER
  Start_log_event_v3();
#ifdef HAVE_REPLICATION
  int pack_info(Protocol* protocol);
#endif /* HAVE_REPLICATION */
#else
  Start_log_event_v3()
  : Log_event(header(), footer())
  {
  }

#endif

  Start_log_event_v3(const char* buf, uint event_len,
                     const Format_description_event* description_event);
  ~Start_log_event_v3() {}
#ifdef MYSQL_SERVER
  bool write(IO_CACHE* file);
#endif
  size_t get_data_size()
  {
    return Binary_log_event::START_V3_HEADER_LEN; //no variable-sized part
  }

protected:
#if defined(MYSQL_SERVER) && defined(HAVE_REPLICATION)
  virtual int do_apply_event(Relay_log_info const *rli);
  virtual enum_skip_reason do_shall_skip(Relay_log_info*)
  {
    /*
      Events from ourself should be skipped, but they should not
      decrease the slave skip counter.
     */
    if (this->server_id == ::server_id)
      return Log_event::EVENT_SKIP_IGNORE;
    else
      return Log_event::EVENT_SKIP_NOT;
  }
#endif
};


/**
  @class Format_description_log_event

  For binlog version 4.
  This event is saved by threads which read it, as they need it for future
  use (to decode the ordinary events).
  This is the subclass of Format_description_event

  @internal
  The inheritance structure in the current design for the classes is
  as follows:
                         Binary_log_event
                                 ^
                                 |
                                 |
                                 |
                Log_event  Start_event_v3
                     ^            /\
                     |           /  \
                     |   <<vir>>/    \ <<vir>>
                     |         /      \
                     |        /        \
                     |       /          \
                Start_log_event_v3   Format_description_event
                             \          /
                              \        /
                               \      /
                                \    /
                                 \  /
                                  \/
                       Format_description_log_event
  @endinternal
  @section Format_description_log_event_binary_format Binary Format
*/

class Format_description_log_event: public Format_description_event,
                                    public Start_log_event_v3
{
public:
  /*
    MTS Workers and Coordinator share the event and that affects its
    destruction. Instantiation is always done by Coordinator/SQL thread.
    Workers are allowed to destroy only "obsolete" instances, those
    that are not actual for Coordinator anymore but needed to Workers
    that are processing queued events depending on the old instance.
    The counter of a new FD is incremented by Coordinator or Worker at
    time of {Relay_log_info,Slave_worker}::set_rli_description_event()
    execution.
    In the same methods the counter of the "old" FD event is decremented
    and when it drops to zero the old FD is deleted.
    The latest read from relay-log event is to be
    destroyed by Coordinator/SQL thread at its thread exit.
    Notice the counter is processed even in the single-thread mode where
    decrement and increment are done by the single SQL thread.
  */
  int usage_counter;
  Format_description_log_event(uint8_t binlog_ver, const char* server_ver=0);
  Format_description_log_event(const char* buf, uint event_len,
                               const Format_description_event
                               *description_event);
                      
/*
#ifdef MYSQL_SERVER
  bool write(IO_CACHE* file);
#endif
*/
  bool header_is_valid() const
  {
    return ((common_header_len >= ((binlog_version==1) ? OLD_HEADER_LEN :
                                   LOG_EVENT_MINIMAL_HEADER_LEN)) &&
            (!post_header_len.empty()));
  }

  bool version_is_valid() const
  {
    /* It is invalid only when all version numbers are 0 */
    return !(server_version_split[0] == 0 &&
             server_version_split[1] == 0 &&
             server_version_split[2] == 0);
  }

  size_t get_data_size()
{
    /*
      The vector of post-header lengths is considered as part of the
      post-header, because in a given version it never changes (contrary to the
      query in a Query_log_event).
    */
    return Binary_log_event::FORMAT_DESCRIPTION_HEADER_LEN;
  }
protected:
#if defined(MYSQL_SERVER) && defined(HAVE_REPLICATION)
  virtual int do_apply_event(Relay_log_info const *rli);
  virtual int do_update_pos(Relay_log_info *rli);
  virtual enum_skip_reason do_shall_skip(Relay_log_info *rli);
#endif
};


#ifdef MYSQL_CLIENT
typedef ulonglong my_xid; // this line is the same as in handler.h
#endif



/**
  @class Ignorable_log_event

  Base class for ignorable log events is Ignorable_event.
  Events deriving from this class can be safely ignored
  by slaves that cannot recognize them.

  Its the derived class of Ignorable_event

  @internal
  The inheritance structure is as follows

        Binary_log_event
               ^
               |
               |
 B_l:Ignorable_event     Log_event
                 \       /
       <<virtual>>\     /
                   \   /
             Ignorable_log_event

  B_l: Namespace Binary_log
  @endinternal
*/
class Ignorable_log_event : public virtual binary_log::Ignorable_event,
                            public Log_event
{
public:
/*
#ifndef MYSQL_CLIENT
  Ignorable_log_event(THD *thd_arg)
      : Log_event(thd_arg, LOG_EVENT_IGNORABLE_F,
                  Log_event::EVENT_STMT_CACHE,
                  Log_event::EVENT_NORMAL_LOGGING, header(), footer())
  {
    DBUG_ENTER("Ignorable_log_event::Ignorable_log_event");
    is_valid_param= true;
    DBUG_VOID_RETURN;
  }
#endif
*/
  Ignorable_log_event(const char *buf,
                      const Format_description_event *descr_event);
  virtual ~Ignorable_log_event();
/*
#ifndef MYSQL_CLIENT
  int pack_info(Protocol*);
#endif

*/

  virtual size_t get_data_size() { return Binary_log_event::IGNORABLE_HEADER_LEN; }
};

/**
  @class Rows_query_log_event
  It is used to record the original query for the rows
  events in RBR.
  It is the subclass of Ignorable_log_event and Rows_query_event

  @internal
  The inheritance structure in the current design for the classes is
  as follows:
                         Binary_log_event
                                  ^
                                  |
                                  |
                                  |
                Log_event   B_l:Ignorable_event
                     ^            /\
                     |           /  \
                     |   <<vir>>/    \ <<vir>>
                     |         /      \
                     |        /        \
                     |       /          \
              Ignorable_log_event    B_l:Rows_query_event
                             \          /
                              \        /
                               \      /
                                \    /
                                 \  /
                                  \/
                       Rows_query_log_event

  B_l : namespace binary_log
  @endinternal
*/
class Rows_query_log_event : public Ignorable_log_event,
                             public binary_log::Rows_query_event{
public:
/*
#ifndef MYSQL_CLIENT
  Rows_query_log_event(THD *thd_arg, const char * query, size_t query_len)
    : Ignorable_log_event(thd_arg)
  {
    DBUG_ENTER("Rows_query_log_event::Rows_query_log_event");
    common_header->type_code= binary_log::ROWS_QUERY_LOG_EVENT;
    if (!(m_rows_query= (char*) my_malloc(key_memory_Rows_query_log_event_rows_query,
                                          query_len + 1, MYF(MY_WME))))
      return;
    my_snprintf(m_rows_query, query_len + 1, "%s", query);
    DBUG_PRINT("enter", ("%s", m_rows_query));
    DBUG_VOID_RETURN;
  }
#endif

#ifndef MYSQL_CLIENT
  int pack_info(Protocol*);
#endif
*/
  Rows_query_log_event(const char *buf, uint event_len,
                       const Format_description_event *descr_event);

  virtual ~Rows_query_log_event()
  {
    if (m_rows_query)
      my_free(m_rows_query);
    m_rows_query= NULL;
  }
  


  virtual bool write_data_body(IO_CACHE *file);


  virtual size_t get_data_size()
  {
    return Binary_log_event::IGNORABLE_HEADER_LEN + 1 + strlen(m_rows_query);
  }
#if defined(MYSQL_SERVER) && defined(HAVE_REPLICATION)
  virtual int do_apply_event(Relay_log_info const *rli);
#endif
};


/*
static inline bool copy_event_cache_to_file_and_reinit(IO_CACHE *cache,
                                                       FILE *file,
                                                       bool flush_stream)
{
  return         
    my_b_copy_to_file(cache, file) ||
    (flush_stream ? (fflush(file) || ferror(file)) : 0) ||
    reinit_io_cache(cache, WRITE_CACHE, 0, FALSE, TRUE);
}
*/
#ifdef MYSQL_SERVER
/*****************************************************************************

  Heartbeat Log Event class

  The class is not logged to a binary log, and is not applied on to the slave.
  The decoding of the event on the slave side is done by its superclass,
  binary_log::Heartbeat_event.

 ****************************************************************************/
class Heartbeat_log_event: public binary_log::Heartbeat_event, public Log_event
{
public:
  Heartbeat_log_event(const char* buf, uint event_len,
                      const Format_description_event* description_event);
};

/**
   The function is called by slave applier in case there are
   active table filtering rules to force gathering events associated
   with Query-log-event into an array to execute
   them once the fate of the Query is determined for execution.
*/
bool slave_execute_deferred_events(THD *thd);
#endif

/*
int append_query_string( const CHARSET_INFO *csinfo,
                        String const *from, String *to);
                        */
extern TYPELIB binlog_checksum_typelib;

class Rotate_log_event: public binary_log::Rotate_event, public Log_event
{
public:

  Rotate_log_event(const char* buf, uint event_len,
                   const Format_description_event* description_event);
  ~Rotate_log_event()
  {}
  size_t get_data_size() { return  ident_len + Binary_log_event::ROTATE_HEADER_LEN;}

};
#endif /* _log_event_h */
