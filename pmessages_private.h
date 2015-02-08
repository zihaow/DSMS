
#ifndef _pmessages_private_h_

#include <pthread.h>
#include <semaphore.h>
#include "list.h"

/* Define the internal data structures for the message system */

typedef struct {
  sem_t block_for_receive;  /* A semaphore for a process to wait for messages to arrive. */
  sem_t mailbox_lock;  /* A mutex so that only one thread at a time can add to the mailbox. */
  pthread_t owner;  /* The ID of the thread that can receive messages from this mailbox. */
  List_t mail;  /* A linked list of mail items. */
} mailbox_t;

typedef struct {
  void *message;
  pthread_t sender;
} mail_item_t;

#endif

