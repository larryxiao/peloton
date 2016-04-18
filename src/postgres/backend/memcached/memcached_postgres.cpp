//
// Created by nitin on 4/14/16.
//

#include "miscadmin.h"
#include "../interfaces/libpq/libpq-fe.h" //TODO: Move this to includes
#include "libpq/pqformat.h"
#include "postgres.h"
#include "postmaster/memcached.h"
#include "postmaster/memcached_query_parser.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"

static void parse_select_result_cols(StringInfoData *buf, std::string& result, MC_OP op, int len);

enum MC_OP { GET, SET, ADD, REPLACE};

/* Memcached socket function implementations */
bool MemcachedSocket::refill_buffer() {

  ssize_t bytes_read;

  // our buffer is to be emptied
  buf_ptr = buf_size = 0;

  /* edge case where buffer ends with \r,
   * move it to head
   */
  if (buffer.back() == '\r') {
    buffer[0] = '\r';
    buf_ptr = 1;
    buf_size = 1;
  }

  // return explicitly
  for (;;) {
    //  try to fill the available space in the buffer
    bytes_read = read(port->sock, &buffer[buf_ptr],
                      MC_SOCK_BUFFER_SIZE_BYTES - buf_size );

    if (bytes_read < 0 ) {
      if ( errno == EINTR) {
        // interrupts are OK
        continue;
      }

      // otherwise, report error
      ereport(COMMERROR,
              (errcode_for_socket_access(),
                  errmsg("MC_SOCKET: could not receive data from client: %m")));
      return false;
    }

    if (bytes_read == 0) {
      // EOF, close and return
      return false;
    }

    // read success, update buffer size
    buf_size += bytes_read;

    // reset buffer ptr, to cover special case
    buf_ptr = 0;
    return true;
  }
}

bool MemcachedSocket::read_line(std::string &new_line) {
  std::string rn = "\r\n";
  for (;;) {
    // check if buffer has been exhausted
    if (buf_ptr >= buf_size) {
      // printf("\n Refilling Buffer \n");
      if(!refill_buffer()) {
        // failure, propagate
        return false;
      }

      // printf("\n New Buffer: %s", buffer.c_str());
    }

    // printf("start buf_ptr:%zu, buf_size:%zu\n", buf_ptr, buf_size);
    // search the buffer for \r\n
    auto pos = buffer.find(rn, buf_ptr);
    if ( pos == std::string::npos ) {
      // no \r\n found, concat buf to new_line and continue
      // start from current location, go till end
      size_t substr_size = buf_size-buf_ptr;
      // edge case, last char \r? ignore it temporarily
      if (buffer.back() == '\r') {
        substr_size--;
      }
      new_line += buffer.substr(buf_ptr, substr_size);
      // update buf ptr
      buf_ptr = buf_size;
      // continue till we hit \r\n
      continue;
    } else {
      // printf("Substring:%s (NL)\n", buffer.substr(buf_ptr, pos).c_str());
      // update new line till '\r'
      new_line += buffer.substr(buf_ptr, pos-buf_ptr);

      // printf("buf_ptr: %zu, pos: pos-2 char:%c\n", buffer[pos-2]);
      // update buf ptr location to after \r\n
      buf_ptr = pos+2;

      // printf("New line: %s, Post read, buf_ptr:%zu, buf_size:%zu\n", new_line.c_str(), buf_ptr, buf_size);

      return true;
    }
  }
}

bool MemcachedSocket::write_response(const std::string &response) {
  ssize_t result;
  for (;;) {
    result = write(port->sock, &response[0], response.size());
    if (result < 0) {
      if (errno == EINTR) {
        // interrupts are ok, try again
        continue;
      } else {
        // fatal errors
        return false;
      }
    }

    // weird edge case?
    if (result == 0 && response.size() !=0) {
      // fatal
      return false;
    }


    // we are ok
    return true;
  }
}

void MemcachedMain(int argc, char *argv[], Port *port) {
  // don't terminate yet
  bool terminate = false;
  auto mc_sock = MemcachedSocket(port);
  std::string query_line, query_result;

  sigjmp_buf local_sigjmp_buf;
  volatile bool send_ready_for_query = true;

  const char *dbname = port->database_name;
  const char *username = port->user_name;

  /* Initialize startup process environment if necessary. */
  if (!IsUnderPostmaster) InitStandaloneProcess(argv[0]);

  SetProcessingMode(InitProcessing);

  /*
   * Set default values for command-line options.
   */
  if (!IsUnderPostmaster) InitializeGUCOptions();

  /*
   * Parse command-line options.
   */
  process_postgres_switches(argc, argv, PGC_POSTMASTER, &dbname);

  /* Must have gotten a database name, or have a default (the username) */
  if (dbname == NULL) {
    dbname = username;
    if (dbname == NULL)
      ereport(FATAL,
              (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                  errmsg("%s: no database nor user name specified", progname)));
  }

  /* Acquire configuration parameters, unless inherited from postmaster */
  if (!IsUnderPostmaster) {
    if (!SelectConfigFiles(userDoption, progname)) proc_exit(1);
  }

  /*
   * Set up signal handlers and masks.
   *
   * Note that postmaster blocked all signals before forking child process,
   * so there is no race condition whereby we might receive a signal before
   * we have set up the handler.
   *
   * Also note: it's best not to use any signals that are SIG_IGNored in the
   * postmaster.  If such a signal arrives before we are able to change the
   * handler to non-SIG_IGN, it'll get dropped.  Instead, make a dummy
   * handler in the postmaster to reserve the signal. (Of course, this isn't
   * an issue for signals that are locally generated, such as SIGALRM and
   * SIGPIPE.)
   */
  if (am_walsender)
    WalSndSignals();
  else {
    pqsignal(SIGHUP, SigHupHandler); /* set flag to read config
* file */
    /* Peloton Changes
     * This two handlers are in conflict with the postmaster handlers
     * For the time being, we disable it */
    // pqsignal(SIGINT, StatementCancelHandler);		/* cancel current query
    // */
    // pqsignal(SIGTERM, die); /* cancel current query and exit */

    /*
     * In a standalone backend, SIGQUIT can be generated from the keyboard
     * easily, while SIGTERM cannot, so we make both signals do die()
     * rather than quickdie().
     */
    if (IsUnderPostmaster)
      pqsignal(SIGQUIT, quickdie); /* hard crash time */
    else
      pqsignal(SIGQUIT, die); /* cancel current query and exit */
    InitializeTimeouts();     /* establishes SIGALRM handler */

    /*
     * Ignore failure to write to frontend. Note: if frontend closes
     * connection, we will notice it and exit cleanly when control next
     * returns to outer loop.  This seems safer than forcing exit in the
     * midst of output during who-knows-what operation...
     */
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    pqsignal(SIGUSR2, SIG_IGN);
    pqsignal(SIGFPE, FloatExceptionHandler);

    /*
     * Reset some signals that are accepted by postmaster but not by
     * backend
     */
    pqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some
 * platforms */
  }

  pqinitmask();

  if (IsUnderPostmaster) {
    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&BlockSig, SIGQUIT);
  }

  PG_SETMASK(&BlockSig); /* block everything except SIGQUIT */

  if (!IsUnderPostmaster) {
    /*
     * Validate we have been given a reasonable-looking DataDir (if under
     * postmaster, assume postmaster did this already).
     */
    Assert(DataDir);
    ValidatePgVersion(DataDir);

    /* Change into DataDir (if under postmaster, was done already) */
    ChangeToDataDir();

    /*
     * Create lockfile for data directory.
     */
    CreateDataDirLockFile(false);

    /* Initialize MaxBackends (if under postmaster, was done already) */
    InitializeMaxBackends();
  }

  /* Early initialization */
  BaseInit();

  elog(DEBUG3, "BaseInit() finished");

  /*
   * Create a per-backend PGPROC struct in shared memory, except in the
   * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
   * this before we can use LWLocks (and in the EXEC_BACKEND case we already
   * had to do some stuff with LWLocks).
   */
  // TODO: peloton changes:
  //#ifdef EXEC_BACKEND
  if (!IsUnderPostmaster) InitProcess();  // Doen this in BackendTask
  //#else
  //  InitProcess();
  //#endif
  /* We need to allow SIGINT, etc during the initial transaction */
  PG_SETMASK(&UnBlockSig);

  elog(DEBUG1,
       "PostgresMain :: TopMemoryContext : %p CurrentMemoryContext : %p",
       TopMemoryContext, CurrentMemoryContext);

  /*
   * General initialization.
   *
   * NOTE: if you are tempted to add code in this vicinity, consider putting
   * it inside InitPostgres() instead.  In particular, anything that
   * involves database access should be there, not here.
   */
  InitPostgres(dbname, InvalidOid, username, InvalidOid, NULL);

  // TODO: Peloton Changes
  if (IsBackend == true) {
    StartTransactionCommand();
    peloton_bootstrap();
    CommitTransactionCommand();
  }

  /*
   * If the PostmasterContext is still around, recycle the space; we don't
   * need it anymore after InitPostgres completes.  Note this does not trash
   * *MyProcPort, because ConnCreate() allocated that space with malloc()
   * ... else we'd need to copy the Port data first.  Also, subsidiary data
   * such as the username isn't lost either; see ProcessStartupPacket().
   */
  if (PostmasterContext) {
    MemoryContextDelete(PostmasterContext);
    PostmasterContext = NULL;
  }

  SetProcessingMode(NormalProcessing);

  /*
   * Now all GUC states are fully set up.  Report them to client if
   * appropriate.
   */
  BeginReportingGUCOptions();

  /*
   * Also set up handler to log session end; we have to wait till now to be
   * sure Log_disconnections has its final value.
   */
  if (IsUnderPostmaster && Log_disconnections)
    on_proc_exit(log_disconnections, 0);

  /* Perform initialization specific to a WAL sender process. */
  if (am_walsender) InitWalSender();

  /*
   * process any libraries that should be preloaded at backend start (this
   * likewise can't be done until GUC settings are complete)
   */
  process_session_preload_libraries();

  /*
   * Create the memory context we will use in the main loop.
   *
   * MessageContext is reset once per iteration of the main loop, ie, upon
   * completion of processing of each command message from the client.
   */
  MessageContext = AllocSetContextCreate(
      TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  /*
   * Remember stand-alone backend startup time
   */
  if (!IsUnderPostmaster) PgStartTime = GetCurrentTimestamp();

  /*
   * POSTGRES main processing loop begins here
   *
   * If an exception is encountered, processing resumes here so we abort the
   * current transaction and start a new___ one.
   *
   * You might wonder why this isn't coded as an infinite loop around a
   * PG_TRY construct.  The reason is that this is the bottom of the
   * exception stack, and so with PG_TRY there would be no exception handler
   * in force at all during the CATCH part.  By leaving the outermost setjmp
   * always active, we have at least some chance of recovering from an error
   * during error recovery.  (If we get into an infinite loop thereby, it
   * will soon be stopped by overflow of elog.c's internal state stack.)
   *
   * Note that we use sigsetjmp(..., 1), so that this function's signal mask
   * (to wit, UnBlockSig) will be restored when longjmp'ing to here.  This
   * is essential in case we longjmp'd out of a signal handler on a platform
   * where that leaves the signal blocked.  It's not redundant with the
   * unblock in AbortTransaction() because the latter is only called if we
   * were inside a transaction.
   */
  if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
    /*
     * NOTE: if you are tempted to add more code in this if-block,
     * consider the high probability that it should be in
     * AbortTransaction() instead.  The only stuff done directly here
     * should be stuff that is guaranteed to apply *only* for outer-level
     * error recovery, such as adjusting the FE/BE protocol status.
     */

    /* Since not using PG_TRY, must reset error stack by hand */
    error_context_stack = NULL;

    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /*
     * Forget any pending QueryCancel request, since we're returning to
     * the idle loop anyway, and cancel any active timeout requests.  (In
     * future we might want to allow some timeout requests to survive, but
     * at minimum it'd be necessary to do reschedule_timeouts(), in case
     * we got here because of a query cancel interrupting the SIGALRM
     * interrupt handler.)	Note in particular that we must clear the
     * statement and lock timeout indicators, to prevent any future plain
     * query cancels from being misreported as timeouts in case we're
     * forgetting a timeout cancel.
     */
    disable_all_timeouts(false);
    QueryCancelPending = false; /* second to avoid race condition */

    /* Not reading from the client anymore. */
    DoingCommandRead = false;

    //    /* Make sure libpq is in a good state */
    //    pq_comm_reset();

    /* Report the error to the client and/or server log */
    EmitErrorReport();

    /*
     * Make sure debug_query_string gets reset before we possibly clobber
     * the storage it points at.
     */
    debug_query_string = NULL;

    /*
     * Abort the current transaction in order to recover.
     */
    AbortCurrentTransaction();

    if (am_walsender) WalSndErrorCleanup();

    /*
     * We can't release replication slots inside AbortTransaction() as we
     * need to be able to start and abort transactions while having a slot
     * acquired. But we never need to hold them across top level errors,
     * so releasing here is fine. There's another cleanup in ProcKill()
     * ensuring we'll correctly cleanup on FATAL errors as well.
     */
    if (MyReplicationSlot != NULL) ReplicationSlotRelease();

    /*
     * Now return to normal top-level context and clear ErrorContext for
     * next time.
     */
    MemoryContextSwitchTo(TopMemoryContext);
    FlushErrorState();

    /* We don't have a transaction command open anymore */
    xact_started = false;

    /* Now we can allow interrupts again */
    RESUME_INTERRUPTS();
  }

  /* We can now handle ereport(ERROR) */
  PG_exception_stack = &local_sigjmp_buf;

  /*
   * Non-error queries loop here.
   */

  int i = 0;
  for (;;) {
    /*
     * (1) check for any other interesting events that happened while we
     * slept.
     */
    if (got_SIGHUP) {
      got_SIGHUP = false;
      ProcessConfigFile(PGC_SIGHUP);
    }

    /* check if we need to terminate */
    if (terminate) {
      // TODO: Peloton Changes
      if (IsPostmasterEnvironment == true) {
        MemoryContextDelete(MessageContext);
        MemoryContextDelete(CacheMemoryContext);
      }

      // close the socket
      mc_sock.close_socket();

      proc_exit(0);
      return;
    }

    /*
     * (2) process the command.  But ignore it if we're skipping till
     * Sync.
     */
    // TODO: check when to erase
    query_line.clear();
    query_result.clear();

    if(mc_sock.read_line(query_line)) {
//      printf("\n\nRead line (%d): %s (NEWLINE)\n", ++i, query_line.c_str());

      // TODO parse the Memcached request into calls to the prepared statement
      peloton::memcached::QueryParser qp = peloton::memcached::QueryParser(query_line);//, &mc_sock);

      query_line = qp.parseQuery();
      int op_type = qp.getOpType();

//      printf("op_type:%d\n",op_type);
//      printf("query_line:%s\n",query_line.c_str());
      // get a flag of the operation
      MC_OP op;
      std::string value;

      if(op_type>0){
        if(!mc_sock.read_line(value)){
          op_type=-1;
        }
        else{
          auto temp_loc = query_line.find("$$$$");

//          printf("%s\n",&value[0]);

          char *escaped_value = (char*)malloc( sizeof(char) * (2*value.length() + 1) );

          auto val_size = PQescapeString(escaped_value, &value[0], value.length());

//          printf("%s\n",escaped_value);

          query_line.replace(temp_loc, 4, std::string(escaped_value));
          PQfreemem(escaped_value);
          escaped_value = NULL;
        }
      }

      switch (op_type){
        case 0:{
          op = GET;
          break;
        }
        case 1:{
          op = SET;
          break;
        }
        case 2:{
          op = ADD;
          break;
        }
        case 3: {
          op = REPLACE;
          break;
        }
        case -100:{
          if (!mc_sock.write_response(query_line + "\r\n")) {
//            printf("\nVersion queried, returning dummy \n");
          }
          continue;
        }
        case -101:{
          mc_sock.close_socket();
//          printf("\nQuit/ close socket failed, terminating thread\n");
          terminate = true;

          continue;
        }
        default:{
//          printf("\nRead line failed, terminating thread\n");
//          terminate=true;
          continue;
        }
      }

      auto mc_state = new MemcachedState();
//      printf("Before query run\n");
      exec_simple_query(&query_line[0], mc_state);
//      printf("After query run\n");
      // proceed to frontend write only if response is not empty
      // echo response
      // TODO parse the sql result into Memcached format back to user
      parse_select_result_cols(&mc_state->result, query_result, op, mc_state->result.len);
//      printf("\nMC_RESULT:%s\n",query_result.c_str());
      if (!mc_sock.write_response(query_result + "\r\n")) {
//        printf("\nWrite line failed, terminating thread\n");
        terminate = true;
      }
      // clear the result data
      if(mc_state->result.len > 0)
        pfree(mc_state->result.data);
      delete mc_state;
    } else {
//      printf("\nRead line failed, terminating thread\n");
      // terminate the thread
      terminate = true;
    }

  } /* end of input-reading loop */
}

/* Print all the attribute values for query result
 * in StringInfoData format
 */
static void parse_select_result_cols(StringInfoData *buf, std::string& result, MC_OP op, int len) {
  if (op != GET) {
    result += "STORED";
//    printf("RESULT SAYS NOT GET:%s\n",result.c_str());
  } else {
    // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
    // <data block>\r\n
    if (len == 0) {
      result += "NOT_FOUND";
      return;
    }
    result += "VALUE ";
    buf->cursor = 0;
    // base 2 int, 16 bits
//    buf->len = 2;
    int nattrs = pq_getmsgint(buf, 2);
    if (nattrs != 4) {
//      printf("nattrs:%d != 4\n", nattrs);
      result += "ERROR";
//      printf("RESULT ERROR:%s\n",result.c_str());
      return;
    }
    int col_length;
//    buf->len += 4;
//    buf->cursor += 4;
    // key
    col_length = pq_getmsgint(buf, 4);
//    buf->len += col_length;
    result += pq_getmsgbytes(buf, col_length);
    result += " ";
    // flag
    col_length = pq_getmsgint(buf, 4);
//    buf->len += col_length;
    result += pq_getmsgbytes(buf, col_length);
    result += " ";
    // size
    col_length = pq_getmsgint(buf, 4);
//    buf->len += col_length;
    result += pq_getmsgbytes(buf, col_length);
    result += "\r\n";
    // value
    col_length = pq_getmsgint(buf, 4);
//    buf->len += col_length;
    result += pq_getmsgbytes(buf, col_length);
    result += "\r\nEND";
//    printf("RESULT no ERROR:%s\n",result.c_str());
    return;
  }
}


