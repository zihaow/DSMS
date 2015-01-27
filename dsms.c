//
//  dsms.c
//  handling data input from stream generator.
//
//  Created by Zihao Wu on 2015-01-19.
//
//

#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

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
int rc;

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
    int i;
    for(i=0; i<argc; i++){
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
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
    
    printf("%s",data2);
    
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
        
        //to handle user command.
        printf("Enter the sql command.\n");
        fgets(text,sizeof text, stdin);
        
        rc = sqlite3_exec(db, text, callback, 0, &zErrMsg);
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
