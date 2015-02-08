
#ifndef _list_h_
#define _list_h_

typedef struct list_node {
  void             *data;
  struct list_node *next;
  struct list_node *prev;
} List_node_t;

typedef struct {
  List_node_t *head;
  List_node_t *tail;
} List_t;

int  List_init ( List_t *list );
void List_destroy( List_t *list );

int List_add_tail ( List_t *list, void *data );
int List_add_head ( List_t *list, void *data );
int List_remove_head ( List_t *list, void **data );
int List_head_info ( List_t *list, void **data );
int List_next_node ( List_t *list, void **context, void **data );
int List_remove_in_context ( List_t *list, void **context, void **data );
int List_add_after ( List_t *list, void **context, void *data );

#endif /* _list_h_ */

