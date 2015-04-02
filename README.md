Documentation for DSMS system
DSMS system
C files

- sg.c: stream generator, which reads input from a file and then output one line periodically to be read by dsms.c to build the database. To be specific, it requires the process ID of dsms.c in order to send file and signal to build the database. More specific information can be found on sg.txt, which is the testing result and it records the whole execution process of sg.c.

- dsms.c: data stream management system, it first reads from a config file to get the information required to set up the database, including the name of the database, where to read when receive different signals. When it receives signals from sg.c, then it reads command from output file generated by sg.c periodically. It also allows users to insert data to database and users can also read the content of the database by entering different SQL commands. Also，DSMS now can perform multithreading by allowing user to enter continuous query and output three kinds of formats. User can also enter “quit” to cleanly exit the DSMS system and by entering “stop n” to stop a particular thread that is running (n is phread id which will be displayed on the screen). Query thread will periodically send data via a messaging system and then its formatter thread will output different types of document layer to text file. If users enter new data to the DSMS, then as long as continuous query is running, then its output file will always have the latest data. Please note that, users can insert data to database if and only if when they logged in to the terminal, they change the mode of the database, otherwise they will get an error message of “read-only”. They will need to enter the following command in order to insert data to the database: “chmod 777 <db.folder>”.
	
TXT files. (optional files for execution)
-config.txt: information required to set up the database.
-rates.txt: input source of SIGUSR1.
-info.txt: input source of SIGUSR2.
-dsms.txt: testing results and demonstration of dsms.c working process.
-sg.txt: testing results and demonstration of sg.c working process.
-makeFile: to compile programs before execution.

To execute the systems, first execute dsms by entering “./dsms config.txt”, then uses “ps” to get the process id of dsms, then opens a new terminal window, type in “./sg <inputFile> <data_period> <outputFile> <interrupt_number> <process_ID_of DSMS>.

To begin a continuous query:
- Example: 
- > every 20 format csv to sum1.txt report select * from course
- > every 15 format fixed-width to sum2.txt report select * from course
- > every 5 format key-pairs to sum3.txt report select * from course

- > stop 1 (will stop thread 1, including query and formatter thread.)
- > quit (this will exit the DSMS system)



To execute the systems, first execute dsms by entering “./dsms config.txt”, then uses “ps” to get the process 
  id of dsms, then opens a new terminal window, 
  type in “./sg <inputFile> <data_period> <outputFile> <interrupt_number> <process_ID_of DSMS>.
  
- LICENSE
- Licensed under MIT: (http://opensource.org/licenses/MIT)
