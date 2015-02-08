
/* File list.c implements a generic doubly-linked-list.  All data is
   considered to be void pointers and is shuffled around as such. */

#include <stdlib.h>
#include "list.h"
#include "sqlite3.h"

/* Initialize a list data structure */

int  
List_init ( List_t *list )
{
  int all_ok = 0;

  /* Set the list information to NULL pointers to indicate that there
     is no data in the list. */

  if (list != NULL) {
    list->head = NULL;
    list->tail = NULL;
    all_ok = 1;
  }

  return all_ok;
}


/* Delete all elements in a list and release all memory associated with
   the list. */

void 
List_destroy( List_t *list )
{
  List_node_t *temp = NULL;

  /* Free up all the nodes that currently exist in the list then reset
     the list information as if the list was just initialized. */

  if (list != NULL) {
    while (list->head != NULL) {
      temp = list->head;
      list->head = list->head->next;
      free( temp );
    }
    list->head = NULL;
    list->tail = NULL;
  }
}


/* Add an element to the end of a list.  The list behaves as a FIFO. */

int 
List_add_tail ( List_t *list, void *data )
{
  int         all_ok = 0;
  List_node_t *new_node = NULL;

  if (list != NULL) {
    /* Create a new node for the data. */

    new_node = (List_node_t *) malloc( sizeof( List_node_t ) );
    if (new_node != NULL) {
      new_node->next = NULL;
      new_node->prev = list->tail;
      new_node->data = data;

      /* Add the new node to the tail of the list. */

      if (list->tail == NULL) {
        list->head = new_node;
      } else {
	list->tail->next = new_node;
      }
      list->tail = new_node;

      all_ok = 1;
    }
  }

  return all_ok;
}

/* Add an element to the front of a list.  The list behaves as a LIFO. */

int 
List_add_head ( List_t *list, void *data )
{
  int         all_ok = 0;
  List_node_t *new_node = NULL;

  if (list != NULL) {
    /* Create a new node for the data. */

    new_node = (List_node_t *) malloc( sizeof( List_node_t ) );
    if (new_node != NULL) {
      new_node->next = list->head;
      new_node->prev = NULL;
      new_node->data = data;

      /* Add the new node to the head of the list. */

      if (list->head == NULL) {
        list->tail = new_node;
      } else {
	list->head->prev = new_node;
      }
      list->head = new_node;

      all_ok = 1;
    }
  }

  return all_ok;
}


/* Remove the element at the front of the list. */

int 
List_remove_head ( List_t *list, void **data )
{
  int         all_ok = 0;
  List_node_t *temp = NULL;

  if ((list != NULL) && (data != NULL)) {
    if (list->head != NULL) {
      /* Get the data */

      *data = list->head->data;

      /* Remove the node */

      temp = list->head;
      list->head = temp->next;
      if (list->head == NULL) {
        list->tail = NULL;
      } else {
	list->head->prev = NULL;
      }

      free( temp );

      all_ok = 1;
    } else {
      *data = NULL;
    }
  }

  return all_ok;
}


/* Return the information at the front of the list without
   removing it from the list itself.  It is the non-destructive
   version of List_remove. */

int 
List_head_info ( List_t *list, void **data )
{
  int all_ok = 0;

  *data = NULL;
  if ((list != NULL) && (list->head != NULL)) {
    *data = list->head->data;
    all_ok = 1;
  }

  return all_ok;
}


/* Allow code to traverse through a list linearly.  The context
   parameter is state that the calling program stores for the
   traversal.  It indicates the last node in the list that was
   reported by the traversal.  To begin a traversal, pass a NULL
   value as the context content (but not the context variable itself). */

int
List_next_node ( List_t *list, void **context, void **data )
{
  int all_ok = 1;
  List_node_t **current_node = (List_node_t **) context;

  /* Determine the next node to report in the list. */

  if (*current_node == NULL) {
    *current_node = list->head;
  } else {
    *current_node = (*current_node)->next;
  }

  /* If we haven't hit the end of the list, report the data at that node. */

  *data  = NULL;
  if (*current_node != NULL) {
    *data = (*current_node)->data;
  }

  return all_ok;
}


/* Remove a particular element from a list.  This is typically used
   in conjunction with the next_node function.  The context performs the
   same role...identifying the list element to remove.  The data in 
   the deleted element is returned in case the calling function has not
   cached it or needs to destroy it separately. */

int
List_remove_in_context ( List_t *list, void **context, void **data )
{
  int all_ok = 1;
  List_node_t **node = (List_node_t **) context;
  List_node_t *tmp;

  *data = NULL;
  if ((node == NULL) || (*node == NULL)) {
    all_ok = 0;
  } else {
    /* There is data to remove. */

    if ((*node)->prev == NULL) {
      /* It is at the head of the list. */
      list->head = (*node)->next;
    } else {
      /* Partially disconnect the node from the list. */

      (*node)->prev->next = (*node)->next;
    }

    if ((*node)->next == NULL) {
      /* It is at the tail of the list. */
      list->tail = (*node)->prev;
    } else {
      /* Complete the disconnect from the list. */

      (*node)->next->prev = (*node)->prev;
    }

    /* Now the list is reset.  Return the data in the node
       and move the node so that a subsequent call to "next"
       will continue to progress down the list. */

    *data = (*node)->data;
    tmp = *node;
    *node = tmp->prev;
    free( tmp );
  }

  return all_ok;
}


/* Add a particular element to a list at one place.  This is typically used
   in conjunction with the next_node function.  The context performs the
   same role...identifying the list element to add after.  */

int
List_add_after ( List_t *list, void **context, void *data )
{
  int all_ok = 1;
  List_node_t **node = (List_node_t **) context;
  List_node_t *tmp;

  if (node == NULL) {
    all_ok = 0;
  } else {
    /* There is a list available. */

    if (*node == NULL) {
      /* We are adding to the list.  Use the regular function for this. */

      all_ok = List_add_head( list, data );
    } else {
      /* We are adding inside the list. */

      tmp = (List_node_t *) malloc( sizeof( List_node_t ) );
      if (tmp != NULL) {
	tmp->data = data;
	tmp->prev = *node;
	tmp->next = (*node)->next;

	/* Insert it into the list. */

	(*node)->next = tmp;
	if (tmp->next != NULL) {
	  tmp->next->prev = tmp;
	} else {
	  list->tail = tmp;
	}
      }
    }
  }

  return all_ok;
}

