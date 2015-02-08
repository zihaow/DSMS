
#ifndef _pmessages_h_

#include <pthread.h>

/* Standard return codes for this system. */

#define MSG_FAIL (0)
#define MSG_OK (1)

/* Define all of the functions that can be used. */

int messages_init ();
void messages_end ();

/* The message sending process takes no responsibility for any memory used by the messages themselves. */

int send_message_to_thread( pthread_t thread, void *message );

/* Each of thread and messageshas values returning to the caller, so you need to pass in addresses. */

int receive_message( pthread_t *thread, void **message );

#endif

