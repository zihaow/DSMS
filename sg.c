//  class name: stream generator
//  class: sg.c
//  
//
//  Created by Zihao Wu on 2015-01-19.
//
//

#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include "string.h"
#include <sqlite3.h>

int main(int argc, char **argv){

    //set the data_periodicity
    int data_period = 0;
    data_period = atoi(argv[2]);
    
    //get the interrupt number from arguments.
    int interrupt_number = 0;
    interrupt_number = atoi(argv[4]);
    
    //to open the file.
    FILE *file = fopen(argv[1],"r");
    
    //initialize data size.
    char data[100];
   
    //create a new file for output.
    FILE *fp;
   
    //initialize line number.
    int lines = 0;
    
    //get the number of lines in input file
    while(fgets(data,sizeof data, file)!= NULL){
        lines++;
    }
    fclose(file);
    
    //reopen the file for reading.
    FILE *file2 = fopen(argv[1],"r");
    
    while (lines != 0) {
        if(argc == 6){
            int id = atoi(argv[5]);
            fp = fopen(argv[3],"w");
            
            //check for file error case
            if(fp == NULL){
                printf("Error opening filr.\n");
                exit(1);
            }
            
            //get a line for output
            fgets(data,sizeof data, file2);
            fprintf(fp,"%s",data);
            
            //close the file
            fclose(fp);
            
            //send interrupt.
            kill(id, interrupt_number);
            sleep(data_period);
            printf("%s", data);
        }
        lines--;
    }
    
    //close the files
    fclose(file2);
}
