/*
 * dsms.c
 * handling data input from stream generator.
 * Created by Zihao Wu on 2015-01-19.
 */

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "pmessages.h"
#include "list.h"

sqlite3 *db;
FILE *file;
FILE *file2;
FILE *cf;
FILE *fp;
char *zErrMsg = 0;
char data[1000];
char data2[1000];
char dbname[200];
char secondfile[20];
char thirdfile[200];
char sqlC[100];
char timeLength[50];
char formatType[50];
char outputFile[50];
int rc;

/* to store each thread's info. */
typedef struct{
    int time;
    char formatTypeS[50];
    char outputFileName[50];
    char sqlCommand[100];
    pthread_t format_thread;
}Threads_dsms;

/* to track thread id. */
typedef struct{
    pthread_t query_id;
    pthread_t format_thread;
}Threads_id;

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
    int i;
    for(i=0; i<argc; i++){
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

/* Mainly uses as messaging passing system. */
static int callback2(void *ths, int argc, char **argv, char **azColName){
    
    int i;
    char *two;
    pthread_t formatting_thread = ((Threads_dsms *)ths) -> format_thread;
    
    for(i=0; i<argc; i++){
        two = (char *) malloc(100*sizeof(char));
        sprintf(two, "\"%s\" \"%s\"\n", azColName[i], argv[i] ? argv[i] : "NULL");
        if(send_message_to_thread(formatting_thread,(void *)two ) != MSG_OK){
            printf( "second 1 failed\n" );
        }
        else{
            //printf("Continuous query Thread %d is running.\n",threads-1);
            //printf( "Message sent successfully.\n" );
        }
    }
    printf("\n");
    return 0;
}

/* to manager thread output. */
void report(Threads_dsms *ths){
    
    while(1){
        sleep(ths->time);
        rc = sqlite3_open(dbname, &db);
        if( rc ){
            fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
        }
        
        rc = sqlite3_exec(db, ths->sqlCommand, callback2, (void *)ths, &zErrMsg);
        
        if( rc!=SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }
        sqlite3_close(db);
    }
}

/* To format different output type. */
void format(Threads_dsms *ths){
    
    pthread_t received;
    char *comeback;
    int initial = 1;
    
    /* file to output. */
    FILE *fileToOut;
    fileToOut = fopen(((Threads_dsms *)ths)->outputFileName, "w");
    
    /* detect output types. */
    char typeFormat1[4] = "csv",typeFormat2[12] = "fixed-width",typeFormat3[10] = "key-pairs";
    
    while(1){
        comeback = (char *) malloc(100*sizeof(char));
        
        if (receive_message(&received, (void **)(&comeback) ) == MSG_OK) {
            printf ("received message 1--%s\n", comeback );
            
            printf("**%s**\n",((Threads_dsms *)ths)->formatTypeS);
            
            /* handle diferent format outputs. */
            /* CSV format output */
            if(strcmp((((Threads_dsms *)ths)->formatTypeS), typeFormat1) == 0){
                printf("CSV-------------------------------------------\n");
                int i = 0,count = 0;
                char string1[20];
                char string2[20];
                int length = 0;
                length = strlen(comeback);
                while (comeback[i] != '\0') {
                    if(comeback[i] == ' '){
                        count++;
                        if(count == 1){
                            strncpy(string1,comeback,i);
                            string1[i]='\0';
                            strncpy(string2,comeback+i+1,length);
                            string2[length-i]='\0';
                            strcat(string1,",");
                        }
                    }
                    i++;
                }
                strcat(string1,string2);
                
                //check for file error case
                if(fileToOut == NULL){
                    printf("Error opening filr.\n");
                    exit(1);
                }
                
                //write to file.
                fprintf(fileToOut, "%s", string1);
            }
            
            /* Fixed-width format output. */
            if(strcmp((((Threads_dsms *)ths)->formatTypeS), typeFormat2) == 0){
                printf("FIXED-WIDTH-----------------------------------\n");
                int i2=0,count2 = 0;
                char string3[20];
                char string4[20];
                int q12=0,q22=0,q32=0,q42=0;
                int length2 = 0;
                length2 = strlen(comeback);
                while (comeback[i2] != '\0') {
                    if(comeback[i2] == '"'){
                        count2++;
                        if(count2 == 1){
                            q12=i2;
                        }
                        if(count2 == 2){
                            q22 = i2;
                            strncpy(string3,comeback+1,q22-1);
                            string3[q22-1]='\0';
                        }
                        if(count2 == 3){
                            q32 = i2;
                        }
                        if(count2 == 4){
                            q42 = i2;
                            strncpy(string4,comeback+q32+1,q42-q32-1);
                            string4[q42-q32-1]='\0';
                        }
                    }
                    i2++;
                }
                int string3Length = 0;
                string3Length = strlen(string3);
                char add[15];
                strcat(string3,add);
                string3[15]='\0';
                
                int string4Length = 0;
                string4Length = strlen(string4);
                char add2[15] = " ";
                strcat(string4,add2);
                string4[15]='\0';
                
                strcat(string3,string4);
                
                char stringOdd[80];
                char stringEven[80];
                
                if(initial%2 == 0){
                    strcpy(stringEven,string3);
                    printf("even%s\n",stringEven);
                    if(initial >= 2){
                        strcat(stringOdd,stringEven);
                    }
                }
                else{
                    strcpy(stringOdd,string3);
                }
                
                if(fileToOut == NULL){
                    printf("Error opening filr.\n");
                    exit(1);
                }
                
                int height=0;
                height = strlen(stringOdd);
                
                if(height > 15){
                    fprintf(fileToOut, "%s\n", stringOdd);
                }
            }
            else{
                printf("No new message\n");
            }
            
            /* Key-pairs fotmat output. */
            if(strcmp((((Threads_dsms *)ths)->formatTypeS), typeFormat3) == 0){
                printf("KEY-PAIRS-------------------------------------\n");
                int i3=0,count3 = 0;
                char string5[20];
                char string6[20];
                int q13=0,q23=0,q33=0,q43=0;
                int length3 = 0;
                length3 = strlen(comeback);
                while (comeback[i3] != '\0') {
                    if(comeback[i3] == '"'){
                        count3++;
                        if(count3 == 1){
                            q13=i3;
                        }
                        if(count3 == 2){
                            q23 = i3;
                            strncpy(string5,comeback+1,q23-1);
                            string5[q23-1]='\0';
                        }
                        if(count3 == 3){
                            q33 = i3;
                        }
                        if(count3 == 4){
                            q43 = i3;
                            strncpy(string6,comeback+q33+1,q43-q33-1);
                            string6[q43-q33-1]='\0';
                        }
                        
                    }
                    i3++;
                }
                strcat(string5,"=");
                strcat(string5,string6);
                
                if(fileToOut == NULL){
                    printf("Error opening filr.\n");
                    exit(1);
                }
                fprintf(fileToOut, "%s\n", string5);
            }
        }
        initial++;
    }
}

/* config method: to open config file for rereading. */
void config(){
    printf("signal received.\n");
    cf = fopen("config.txt","r");
}

/* receive method: to handle SIGUSR1 interrupt. Then transfer data to database. */
void receive(){
    
    file = fopen(secondfile,"r");
    fgets(data,sizeof data, file);
    fclose(file);
    printf("%s",data);
    
    rc = sqlite3_open(dbname, &db);
    
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
    }
    
    rc = sqlite3_exec(db, data, callback, 0, &zErrMsg);
    
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
}

/* receive 2 method: to handle SIGUSR2 interrupt. 
 Then transfer data to database.*/
 void receive2(){
    
    file2 = fopen(thirdfile,"r");
    fgets(data2,sizeof data2, file2);
    fclose(file2);
    
     rc = sqlite3_open(dbname, &db);
    
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
    }
    
    rc = sqlite3_exec(db, data2, callback, 0, &zErrMsg);
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
}


/* alarm_receive method: to handle the alarm 
 interrupt to clean up the database periodically. */
void alarm_receive(){
    int status;
    status = remove(dbname);
}

int main(int argc, char **argv){
    
    int threads = 1;
    int time2 = 0;
    messages_init();
    List_t thread_id;
    List_init(&thread_id);
    int return_code = 0;
    char dbnameS[200];
    char initfileS[100];
    char initfile[20];
    char secondfileS[200];
    char thirdfileS[200];
    char intValueS[20];
    char intValue[20];
    
    /* check to make sure user enter the command correctly. */
    if( argc!=2 ){
        fprintf(stderr, "Configuration file not found.%s\n", argv[0]);
        return(1);
    }
    
    /* to open the config file. */
    cf = fopen(argv[1],"r");
    char info[100];
    
    /* check for file error case */
    if(cf == NULL){
        printf("Error opening filr.\n");
        exit(1);
    }
    
    /* get the database name. */
    fgets(dbnameS,sizeof dbnameS, cf);
    int len = strlen(dbnameS);
    strncpy(dbname,dbnameS+3,len-2);
    
    /* get the initial file name. */
    fgets(initfileS,sizeof initfileS, cf);
    int len2 = strlen(initfileS);
    strncpy(initfile,initfileS+5,len2-2);
    
    /* get the user1 file name. */
    fgets(secondfileS,sizeof secondfileS, cf);
    int len3 = strlen(secondfileS);
    strncpy(secondfile,secondfileS+5,len3-2);
    
    /* get the user2 file name. */
    fgets(thirdfileS,sizeof thirdfileS, cf);
    int len4 = strlen(thirdfileS);
    strncpy(thirdfile,thirdfileS+5,len4-2);
    
    /* get the time to clean database data. */
    fgets(intValueS,sizeof intValueS, cf);
    int len5 = strlen(intValueS);
    strncpy(intValue,intValueS+6,len5-2);
    int clean = atoi(intValue);
    
    /* to remove '\n' after filename. */
    strtok(secondfile,"\n");
    strtok(thirdfile,"\n");
    
    /* close the configuration file. */
    fclose(cf);
    
    /* handle SIGUSR1 interrupt. */
    if(signal(SIGUSR1, receive) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
    
    /* handle SIGUSR2 interrupt. */
    if(signal(SIGUSR2, receive2) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
    
    /* handle SIGHUP interrupt. */
    if(signal(SIGHUP, config) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
    
    /* handle ALARM interrupt to clean the database periodically. */
    if(signal(SIGALRM, alarm_receive) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
    else{
        alarm(clean);
    }
    
    /* get user input. */
    char text[300];
    pthread_t query_id;
    pthread_t format_thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    while(1){
        
        int th = 0;
        rc = sqlite3_open(dbname, &db);
        
        /* to handle user command. */
        printf("Enter the sql command.\n");
        fgets(text,sizeof text, stdin);
        strtok(text,"\n");
        
        rc = sqlite3_exec(db, text, callback, 0, &zErrMsg);
        
        char quit[5] = "quit",every[5] = "ever",stop[5] = "stop";
        char check[5];
        strncpy(check,text,4);
        check[4] = '\0';
        
        /* to handle quit command. */
        if(strcmp(text, quit) == 0){
            exit(0);
        }
        
        /* to handle continuous query command. */
        if(strcmp(check, every) == 0){
            
            int count =0,i=0,j=0,k=0,k2=0,starting=0;
            Threads_dsms *node = (Threads_dsms *)malloc(sizeof(Threads_dsms));
            Threads_id *idNode  = (Threads_id *)malloc(sizeof(Threads_id));
            
            /* to get the time for output. */
            while (text[j] != '\0') {
                if(text[j] == ' '){
                    count++;
                    if(count == 1){
                        strncpy(timeLength,text+6,j-1);
                        time2 = atoi(timeLength);
                    }
                }
                j++;
            }
            count = 0;
            node -> time = time2;
            
            /* to get the format type. */
            int be = 0;
            int en = 0;
            while (text[k] != '\0') {
                if(text[k] == ' '){
                    count++;
                    if(count == 3){
                        be = k;
                    }
                    if(count == 4){
                        en = k;
                    }
                }
                k++;
            }
            strncpy(formatType,text+be+1,en-be-1);
            count = 0;
            strncpy(node->formatTypeS, formatType, sizeof(node->formatTypeS));
            
            /* to get the output file name. */
            int end =0;
            int begin =0;
            while (text[k2] != '\0') {
                if(text[k2] == ' '){
                    count++;
                    if(count == 5){
                        begin = k2;
                    }
                    if(count == 6){
                        end = k2;
                    }
                }
                k2++;
            }
            strncpy(outputFile,text+begin+1,end-begin-1);
            count = 0;
            strncpy(node->outputFileName, outputFile, sizeof(node->outputFileName));
            
            /* to get sql command. */
            while (text[i] != '\0') {
                if(text[i] == ' '){
                    count++;
                    starting = i;
                    if(count == 7){
                        strncpy(sqlC,text+starting+1,100);
                    }
                }
                i++;
            }
            strncpy(node->sqlCommand, sqlC, sizeof(node->sqlCommand));
            
            /* create query threads. */
            th = pthread_create(&query_id, &attr, (void *)report, node);
            idNode -> query_id = query_id;
            List_add_tail(&thread_id, (void *)idNode);
            
            /* create formatting threads. */
            pthread_create(&format_thread, &attr, (void *)format, node);
            node -> format_thread = format_thread;
            
            idNode -> format_thread = format_thread;
            threads++;
            printf("Continuous thread Thread %d is running.\n",threads);
        }
        
        /* Users must check which thread's id is running, if it is 1, then
         simply enter "stop 1" to stop thread 1 (Continuous query) and 
         thread 2 (Formatting thread), if 3, then
         enter "stop 3", etc. If not entered according to latetst undated
         thread id, segmentation fault will be reached. */
        
        if(strcmp(check, stop) == 0){
            
            int i1 = 0,t=0,counts=0;
            char threadID[100];
            int textLength = 0;
            textLength = strlen(text);
            strncpy(threadID,text+4,textLength);
            t = atoi(threadID);
            
            void *thread_ids;
            void *previous_thread_ids;
            
            for(i1=0; i1<t;i1++){
                List_next_node(&thread_id, &previous_thread_ids, &thread_ids);
            }
            /* to stop the continuous query and formatting thread. */
        
            pthread_t query_id = ((Threads_id *)thread_ids) -> query_id;
            pthread_t format_thread = ((Threads_id *)thread_ids) -> format_thread;
            printf("Stopping the thread,\n");
            pthread_cancel(query_id);
            pthread_cancel(format_thread);
        }
    
        if( rc!=SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }
    }
    
    /* Close access to the database so someone else can use it. */
    sqlite3_close(db);
    
    //fclose(file);
    return return_code;
}