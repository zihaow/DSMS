
#include <pthread.h>
#include <semaphore.h>
#include <strings.h>
#include <stdlib.h>
#include "list.h"
#include "pmessages_private.h"
#include "pmessages.h"
#include "sqlite3.h"

static List_t mailboxes;
static sem_t  mailboxes_lock;

static mailbox_t *
new_mailbox( pthread_t owner )
{
  mailbox_t *mailbox = NULL;

  /* Allocate the mailbox with no mail items and initialize its fields. */

  mailbox = (mailbox_t *) malloc( sizeof( mailbox_t ) );
  if (mailbox != NULL) {
    mailbox->owner = owner;
    sem_init( &(mailbox->block_for_receive), 0, 0);
    sem_init( &(mailbox->mailbox_lock), 0, 1);

    if (!List_init( &(mailbox->mail) )) {
      /* We couldn't get our list going.  Get rid of what we've done already and fail the function. */

      sem_destroy( &(mailbox->block_for_receive) );
      sem_destroy( &(mailbox->mailbox_lock) );
    
      free( mailbox );
      mailbox = NULL;
    }
  }

  return mailbox;
}

static void
destroy_mailbox( mailbox_t *mailbox )
{
  void *mail_context = NULL;
  mail_item_t *mail;

  if (mailbox != NULL) {
    sem_wait( &(mailbox->mailbox_lock) );
    /* Get rid of the contents of the mailbox */

    mail_context = NULL;
    while (List_next_node(&mailbox->mail, &mail_context, (void **)(&mail)) && (mail != NULL)) {
      free( mail->message );
      mail->sender = 0;

      sem_destroy( &(mailbox->block_for_receive) );
      sem_destroy( &(mailbox->mailbox_lock) );

      List_destroy( &mailbox->mail );
      free( mail );
    }

    /* Now I can get rid of this node itself. */

    free( mailbox );
  }
}

static mailbox_t *
locate_mailbox( pthread_t process )
{
  mailbox_t *mailbox = NULL;
  mailbox_t *search_result = NULL;
  void *context = NULL;
  int found = 0;

  /* Get sole priority to the set of mailboxes before starting. */

  sem_wait( &(mailboxes_lock) );

  /* Do a linear search through the mailbox list until we find the one that we need. */

  context = NULL;
  found = 0;
  while (!found && List_next_node(&mailboxes, &context, (void **)(&search_result)) && (search_result != NULL)) {
    if (search_result->owner == process) {
      /* We have the mailbox that was asked for. */
      mailbox = search_result;
      found = 1;
    }
  }

  if (!found) {
    /* Create a mailbox for this process. */
    mailbox = new_mailbox( process );
    if (mailbox != NULL) {
      if (!List_add_tail ( &mailboxes, (void *)mailbox )) {
	/* We can't add it, so get rid of it. */

        destroy_mailbox( mailbox );
	mailbox = NULL;
      }
    }
  }

  /* Release the set of mailboxes to anyone else. */

  sem_post( &(mailboxes_lock) );

  return mailbox;
}

int 
messages_init ( void )
{
  int return_code = MSG_FAIL;

  /* The only initialization is getting a list ready for the mailboxes
     and creating a lock for that list. */

  if (List_init( &mailboxes )) {
    sem_init( &mailboxes_lock, 0, 1);
    return_code = MSG_OK;
  }

  return return_code;
}


void 
messages_end ( void )
{
  void *context = NULL;
  mailbox_t *mailbox;

  /* Claim the list before we actually stop the world. */

  sem_wait( &mailboxes_lock );

  /* The list is ours, so we can tear it down. */

  /* Start by eliminating the list contents. */

  context = NULL;
  while (List_next_node(&mailboxes, &context, (void **)(&mailbox)) && (mailbox != NULL)) {
    destroy_mailbox( mailbox );
  }
}


int 
send_message_to_thread( pthread_t destination, void *message )
{
  int return_code = MSG_FAIL;
  mailbox_t *mailbox;
  mail_item_t *mail;

  /* Find the destination. */

  mailbox = locate_mailbox( destination );
  if (mailbox != NULL) {
    /* Create the message to leave. */

    mail = (mail_item_t *)malloc( sizeof( mail_item_t ) );
    if (mail != NULL) {

      /* Store the message to pass along. */

        mail->message = message;
        mail->sender = pthread_self();

        /* Get sole access to the mailbox. */

        sem_wait( &(mailbox->mailbox_lock) );
        if (List_add_tail ( &(mailbox->mail), (void *)mail)) {
	  /* It's ready!.  Unblock a process if it is waiting for a message. */
  
	  sem_post( &(mailbox->block_for_receive) );
	  return_code = MSG_OK;
        }

        /* Relinquish access to the mailbox. */

        sem_post( &(mailbox->mailbox_lock) );

        /* If we failed, clean up after ourselves. */

        if (return_code != MSG_OK) {
	  free( mail->message );
	  mail->message = NULL;
	  mail->sender = 0;
	  free( mail );
        }
    }
  }

  return return_code;
}


int 
receive_message( pthread_t *sender, void **message )
{
  int return_code = MSG_FAIL;
  mailbox_t *mailbox;
  mail_item_t *mail;

  /* Find my mailbox . */

  mailbox = locate_mailbox( pthread_self() );
  if (mailbox != NULL) {

    /* Block if nothing is waiting for me. */

    sem_wait( &(mailbox->block_for_receive) );

    /* Get sole access to the mailbox. */

    sem_wait( &(mailbox->mailbox_lock) );
    if (List_remove_head ( &(mailbox->mail), (void **)(&mail))) {
      if (mail != NULL) {
        /* We have mail to return! */

        *sender = mail->sender;
        *message = mail->message;

        free( mail );

        return_code = MSG_OK;
      }
    }

    /* Relinquish access to the mailbox. */

    sem_post( &(mailbox->mailbox_lock) );
  }

  return return_code;
}

/* Create mailboxes if they don't exist rather than failing! */

