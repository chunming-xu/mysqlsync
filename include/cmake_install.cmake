# Install script for directory: /Users/stevenxu/Desktop/src/mysql-5.7.26/include

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/Users/stevenxu/Desktop/run/mysql5.7.26")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xDevelopmentx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE FILE FILES "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/../libbinlogevents/export/binary_log_types.h")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xDevelopmentx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE FILE FILES
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql_com.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_command.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql_time.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_list.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_alloc.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/typelib.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql/plugin.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql/plugin_audit.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql/plugin_ftparser.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql/plugin_validate_password.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql/plugin_keyring.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql/plugin_group_replication.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_dbug.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/m_string.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_sys.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_xml.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql_embed.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_thread.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_thread_local.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/decimal.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/errmsg.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_global.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_getopt.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/sslopt-longopts.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_dir.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/sslopt-vars.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/sslopt-case.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/sql_common.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/keycache.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/m_ctype.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_compiler.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql_com_server.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_byteorder.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/byte_order_generic.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/byte_order_generic_x86.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/little_endian.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/big_endian.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/thr_cond.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/thr_mutex.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/thr_rwlock.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql_version.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/my_config.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysqld_ername.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysqld_error.h"
    "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/sql_state.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xDevelopmentx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/mysql" TYPE DIRECTORY FILES "/Users/stevenxu/Desktop/src/mysql-5.7.26/include/mysql/" REGEX "/[^/]*\\.h$" REGEX "/psi\\_abi[^/]*$" EXCLUDE)
endif()

