//
//  dsms.c
//  handling data input from stream generator.
//
//  Created by Zihao Wu on 2015-01-19.
//
//

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "pmessages.h"

sqlite3 *db;
FILE *file;
FILE *file2;
FILE *cf;
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
int threads = 0;
char *comeback, one[] = "the quick brown fox", two[]="jumps over the lazy dog";

//set maximun number of threads.
pthread_t tid[50];

static int csv(void *NotUsed, int argc, char **argv, char **azColName){
    int i;
    for(i=0; i<argc; i++){
        printf("\"%s, %s\"\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}


static int callback(void *NotUsed, int argc, char **argv, char **azColName){
    int i;
    for(i=0; i<argc; i++){
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}


//to manager thread output.
void* report(void *arg){
    
    printf(" (pthread id %d) has started\n", pthread_self());
    while(1){
        sleep(10);
        tid[threads+1] = pthread_self();
        if(send_message_to_thread(tid[threads+1],(void *)two ) != MSG_OK){
            printf( "second 1 failed\n" );
        }
        else{
            printf( "Message sent successfully.\n" );
        }
    
        rc = sqlite3_open(dbname, &db);
    
        if( rc ){
            fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
        }
    
        rc = sqlite3_exec(db, sqlC, callback, 0, &zErrMsg);
    
        if( rc!=SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }
    }
    //pthread_id_np_t tid;
    //tid = pthread_getthreadid_np();
    //printf(" (pthread id %d) has started\n", pthread_self());
    
    return NULL;
}

void* format(void *arg){
    
    rc = sqlite3_open(dbname, &db);
    
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
    }
    
    rc = sqlite3_exec(db, sqlC, callback, 0, &zErrMsg);
    
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    
    return NULL;
}

/**
 * config method: to open config file for rereading.
 */
void config(){
    printf("signal received.\n");
    cf = fopen("config.txt","r");
}

/**
 * receive method: to handle SIGUSR1 interrupt. Then transfer data to database.
 */
void receive(){
    
    //open the file for input.
    file = fopen(secondfile,"r");
    
    //get the data.
    fgets(data,sizeof data, file);
    
    //close the file
    fclose(file);
    
    printf("%s",data);
    
    //create the database
    rc = sqlite3_open(dbname, &db);
    
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        
        /* Given that the open call failed, I'm not sure why they want to close
         the database.  Maybe just to be sure.  Presumably, if the database isn't
         opent he sqlite3_close does no harm. */
        
        sqlite3_close(db);
    }
    
    /* Run the command that is given as the second argument when starting the
     program.  For each row of output, sqlite3_exec invokes the function
     "callback". */
    
    rc = sqlite3_exec(db, data, callback, 0, &zErrMsg);
    
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
}

/**
 * receive 2 method: to handle SIGUSR2 interrupt. Then transfer data to database.
 */
void receive2(){
    
    //open the file for input.
    file2 = fopen(thirdfile,"r");

    //get the data/
    fgets(data2,sizeof data2, file2);
    
    //close the file
    fclose(file2);
    
    //printf("%s",data2);
    
    //create the database
    rc = sqlite3_open(dbname, &db);
    
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        
        /* Given that the open call failed, I'm not sure why they want to close
         the database.  Maybe just to be sure.  Presumably, if the database isn't
         opent he sqlite3_close does no harm. */
        
        sqlite3_close(db);
    }
    
    /* Run the command that is given as the second argument when starting the
     program.  For each row of output, sqlite3_exec invokes the function
     "callback". */
    
    rc = sqlite3_exec(db, data2, callback, 0, &zErrMsg);
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
}

/**
 * alarm_receive method: to handle the alarm interrupt to clean up the database periodically.
 */
void alarm_receive(){
    int status;
    status = remove(dbname);
}

int main(int argc, char **argv){
    
    int return_code = 0;
    char dbnameS[200];
    char initfileS[100];
    char initfile[20];
    char secondfileS[200];
    char thirdfileS[200];
    char intValueS[20];
    char intValue[20];
    
    //check to make sure user enter the command correctly.
    if( argc!=2 ){
        fprintf(stderr, "Configuration file not found.%s\n", argv[0]);
        return(1);
    }
    
    //to open the config file
    cf = fopen(argv[1],"r");
    char info[100];
    
    //check for file error case
    if(cf == NULL){
        printf("Error opening filr.\n");
        exit(1);
    }
    
    //get the database name.
    fgets(dbnameS,sizeof dbnameS, cf);
    int len = strlen(dbnameS);
    strncpy(dbname,dbnameS+3,len-2);
    
    //get the initial file name.
    fgets(initfileS,sizeof initfileS, cf);
    int len2 = strlen(initfileS);
    strncpy(initfile,initfileS+5,len2-2);
    
    //get the user1 file name.
    fgets(secondfileS,sizeof secondfileS, cf);
    int len3 = strlen(secondfileS);
    strncpy(secondfile,secondfileS+5,len3-2);
    
    //get the user2 file name.
    fgets(thirdfileS,sizeof thirdfileS, cf);
    int len4 = strlen(thirdfileS);
    strncpy(thirdfile,thirdfileS+5,len4-2);
    
    //get the time to clean database data.
    fgets(intValueS,sizeof intValueS, cf);
    int len5 = strlen(intValueS);
    strncpy(intValue,intValueS+6,len5-2);
    int clean = atoi(intValue);
    
    //to remove '\n' after filename.
    strtok(secondfile,"\n");
    strtok(thirdfile,"\n");
    
    //close the configuration file.
    fclose(cf);
    
    //handle SIGUSR1 interrupt.
    if(signal(SIGUSR1, receive) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
    
    //handle SIGUSR2 interrupt.
    if(signal(SIGUSR2, receive2) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
   
    //handle SIGHUP interrupt.
    if(signal(SIGHUP, config) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
    
    //handle ALARM interrupt to clean the database periodically/
    if(signal(SIGALRM, alarm_receive) == SIG_ERR){
        printf ("Unable to install handler\n");
        return_code = 1;
    }
    else{
        alarm(clean);
    }
    
    //get user input
    char text[300];
    while(1){
        
        
        int th = 0;
        
        
        //to handle user command.
        printf("Enter the sql command.\n");
        fgets(text,sizeof text, stdin);
        
        //if(text != NULL){
          //  threads++;
        //}
        
        
        rc = sqlite3_exec(db, text, callback, 0, &zErrMsg);
        
        char quit[5] = "quit",every[5] = "ever",stop[5] = "stop";
        //char create[2] = "c",insert[2] = "i",c2[2] = "C",i2[2] ="I";
        //printf("is:%s\n",quit);
        
        //printf("--------------\n");
        
        char check[5];
        strncpy(check,text,4);
        //printf("is:%s\n",check);
        
        
        //to handle quit command.
        if(strcmp(check, quit) == 0){
            sqlite3_close(db);
            exit(0);
        }
        
        //to get the thread id.
        /*
        int i1 = 0,t=0,counts=0;
        char threadID[100];
        while (text[i1] != '\0') {
            if(text[i1] == ' '){
                counts++;
                if(counts == 1){
                    strncpy(threadID,text+6,i1-1);
                    t = atoi(threadID);
                }
            }
            i1++;
        }
        printf("is%d\n",t);
        */
        
        //to handle continuous query command.
        if(strcmp(check, every) == 0){
            
            int count =0,i=0,j=0,k=0,k2=0,starting=0;
            int time2 = 0;
            
            //to get the time for output.
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
            strncpy(formatType,text+be+1,en-be);
            count = 0;
            
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
            strncpy(outputFile,text+begin+1,end-begin);
            count = 0;
         
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
            
            //create threads.
            th = pthread_create(&(tid[threads]), NULL, &report, NULL);
            
            //pthread_cancel(tid[threads]);
            tid[threads] = pthread_self();
            pthread_create(&(tid[threads+1]), NULL, &format, NULL);
        
        
        }
        
        //to handle stop command.
        if(strcmp(check, stop) == 0){
            pthread_cancel(tid[threads]);
            printf("Stopping the thread.\n");
        }
        
        
        if( rc!=SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }
        
        /*
        if(strcmp(quit,"quit") != 0){
            sqlite3_close(db);
            exit(0);
        }
        else{
            printf("OMG.\n");
        }
         */
        threads++;
        
    }
    
    /* Close access to the database so someone else can use it. */
    sqlite3_close(db);
    
    //fclose(file);
    return return_code;
}
