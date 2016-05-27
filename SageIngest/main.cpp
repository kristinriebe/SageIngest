/*  
 *  Copyright (c) 2016, Kristin Riebe <kriebe@aip.de>, 
 *                      E-Science team AIP Potsdam
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. You may obtain a copy
 *  of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/* Reader for SAGE output in HDF5-format
 */

#include <iostream>
#include "Sage_Reader.h"
#include "Sage_SchemaMapper.h"
#include "sageingest_error.h"
#include <Schema.h>
#include <DBIngestor.h>
#include <DBAdaptorsFactory.h>
#include <AsserterFactory.h>
#include <ConverterFactory.h>
#include <boost/program_options.hpp>

#include <sstream>
#include <vector>

using namespace Sage;
using namespace std;
namespace po = boost::program_options;


int main (int argc, const char * argv[])
{
    string dataFile;
    string mapFile;
    int ngrid;
    int fileNum;
    int swap;
    long maxRows;
    
    int user_blocksize;

    string dbase;
    string table;
    string system;
    string socket;
    string user;
    string pwd;
    string port;
    string host;
    string path;
    uint32_t bufferSize;
    uint32_t outputFreq;

//    bool greedyDelim;
    bool isDryRun = false;
    bool resumeMode;
    bool askUserToValidateRead = true; // can be overwritten by options below
    
    DBServer::DBAbstractor * dbServer;
    DBIngest::DBIngestor * sageIngestor;
    DBServer::DBAdaptorsFactory adaptorFac;


    //build database string
    string dbSystemDesc = "database system to use (";
    
#ifdef DB_SQLITE3
    dbSystemDesc.append("sqlite3, ");
#endif
    
#ifdef DB_MYSQL
    dbSystemDesc.append("mysql, ");
#endif
    
#ifdef DB_ODBC
    dbSystemDesc.append("unix_sqlsrv_odbc, ");
    dbSystemDesc.append("sqlsrv_odbc, ");
    dbSystemDesc.append("sqlsrv_odbc_bulk, ");
    dbSystemDesc.append("cust_odbc, ");
    dbSystemDesc.append("cust_odbc_bulk, ");
#endif
    
    dbSystemDesc.append(") - [default: mysql]");
    
    
    po::options_description progDesc("SageIngest - Ingest binary HDF5 SAGE files into databases\n\nSageIngest [OPTIONS] [dataFile]\n\nCommand line options:");
        
    progDesc.add_options()
                ("help,?", "output help")
                ("data,d", po::value<string>(&dataFile), "datafile to ingest")
                ("system,s", po::value<string>(&system)->default_value("mysql"), dbSystemDesc.c_str())
                ("bufferSize,B", po::value<uint32_t>(&bufferSize)->default_value(128), "ingest buffer size (will be reduced to sytem maximum if needed) [default: 128]")
                ("outputFreq,F", po::value<uint32_t>(&outputFreq)->default_value(100000), "number of rows after which a performance measurement is output [default: 100000]")
                ("dbase,D", po::value<string>(&dbase)->default_value(""), "name of the database where the data is added to (where applicable)")
                ("table,T", po::value<string>(&table)->default_value(""), "name of the table where the data is added to")
                ("socket,S", po::value<string>(&socket)->default_value(""), "socket to use for database access (where applicable)")
                ("user,U", po::value<string>(&user)->default_value(""), "user name (where applicable")
                ("pwd,P", po::value<string>(&pwd)->default_value(""), "password (where applicable")
                ("port,O", po::value<string>(&port)->default_value("3306"), "port to use for database access (where applicable) [default: 3306 (mysql)]")
                ("host,H", po::value<string>(&host)->default_value("localhost"), "host to use for database access (where applicable) [default: localhost]")
                ("path,p", po::value<string>(&path)->default_value(""), "path to a database file (mainly for sqlite3, where applicable)")
                ("mapFile,f", po::value<string>(&mapFile)->default_value(""), "path to the mapping file")
                ("isDryRun", po::value<bool>(&isDryRun)->default_value(0), "should this run be carried out as a dry run (no data added to database)? [default: 0]")
                ("fileNum", po::value<int>(&fileNum)->default_value(0), "number of the data file (e.g. if multiple files per snapshot, mainly for checking purposes)")
                ("blocksize", po::value<int32_t>(&user_blocksize)->default_value(100000), "number of rows to be read in one block (for each dataset); dataset * blocksize * dataType must fit into memory [default: 10000]")
                ("swap,w", po::value<int32_t>(&swap)->default_value(0), "flag for byte swapping (default 0)")
                ("maxRows,m", po::value<int64_t>(&maxRows)->default_value(0), "maximum number of rows to be read (default 0)")
                ("resumeMode,R", po::value<bool>(&resumeMode)->default_value(0), "try to resume ingest on failed connection (turns off transactions)? [default: 0]")
                ("validateSchema,v", po::value<bool>(&askUserToValidateRead)->default_value(1), "ask user to validate the schema mapping [default: 1]")
                ;
    // Attention: many of these options actually are required; boost version 1.42 and above support ->required() (instead of default()), but not older versions;
    // Unfortunately our servers only have boost 1.41 installed, so it would not work there.
    // required options: dbase, table, mapFile, fileNum

    po::positional_options_description posDesc;
    posDesc.add("data", 1);

    //read out the options
    po::variables_map varMap;
    //po::store(po::command_line_parser(argc, argv).options(progDesc).positional(posDesc).run(), varMap);
    po::store(po::command_line_parser(argc, (char **) argv).options(progDesc).positional(posDesc).run(), varMap);
    // --> only compiles at erebos if I include the (char **) cast
    po::notify(varMap);
    
    if (varMap.count("help") || varMap.count("?") || dataFile.length() == 0) {
        cout << progDesc;
        return EXIT_SUCCESS;
    }
    
    cout << "You have entered the following parameters:" << endl;
    cout << "Data file: " << dataFile << endl;
    cout << "DB system: " << system << endl;
    cout << "Buffer size: " << bufferSize << endl;
    cout << "Performance output frequency: " << outputFreq << endl;
    cout << "Database name: " << dbase << endl;
    cout << "Table name: " << table << endl;
    cout << "Socket: " << socket << endl;
    cout << "User: " << user << endl;
    if (pwd.compare("") == 0) {
        cout << "Password not given" << endl;
    } else {
        cout << "Password given" << endl;
    }
    cout << "Port: " << port << endl;
    cout << "Host: " << host << endl;
    if (path != "") {
        cout << "Path: " << path << endl;
    }
    cout << "Blocksize: " << user_blocksize << endl;

    cout << endl;

   
    DBAsserter::AsserterFactory * assertFac = new DBAsserter::AsserterFactory;
    DBConverter::ConverterFactory * convFac = new DBConverter::ConverterFactory;

    // setup schema mapper; need dataset-names in reader to filter out what
    // we won't need
    SageSchemaMapper * thisSchemaMapper = new SageSchemaMapper(assertFac, convFac);     //registering the converter and asserter factories
    //cout << "Mapping file: " << mapFile << endl;
    vector<string> databaseFieldNames;
    databaseFieldNames = thisSchemaMapper->getFieldNames();

    DBDataSchema::Schema * thisSchema;
    thisSchema = thisSchemaMapper->generateSchema(dbase, table);

    //now setup the file reader
    SageReader *thisReader = new SageReader(dataFile, swap, fileNum, user_blocksize, maxRows, databaseFieldNames);
    dbServer = adaptorFac.getDBAdaptors(system);
    
    sageIngestor = new DBIngest::DBIngestor(thisSchema, thisReader, dbServer);
    sageIngestor->setUsrName(user);
    sageIngestor->setPasswd(pwd);

    //settings for different DBs (copy&paste from AsciiIngest)
    if(system.compare("mysql") == 0) {
        sageIngestor->setSocket(socket);
        sageIngestor->setPort(port);
        sageIngestor->setHost(host);
    } else if (system.compare("sqlite3") == 0) {
        sageIngestor->setHost(path);
    } else if (system.compare("unix_sqlsrv_odbc") == 0) {
        sageIngestor->setSocket("DRIVER=FreeTDS;TDS_Version=7.0;");
        //sageIngestor->setSocket("DRIVER=SQL Server Native Client 10.0;");
        sageIngestor->setPort(port);
        sageIngestor->setHost(host);
    } else if (system.compare("sqlsrv_odbc") == 0) {
        sageIngestor->setSocket("DRIVER=SQL Server Native Client 10.0;");
        sageIngestor->setPort(port);
        sageIngestor->setHost(host);
    } else if (system.compare("sqlsrv_odbc_bulk") == 0) {
        //TESTS ON SQL SERVER SHOWED THIS IS VERY SLOW. BUT NO CLUE WHY, DID NOT BOTHER TO LOOK AT PROFILER YET
        sageIngestor->setSocket("DRIVER=SQL Server Native Client 10.0;");
        sageIngestor->setPort(port);
        sageIngestor->setHost(host);
    }  else if (system.compare("cust_odbc") == 0) {
        sageIngestor->setSocket(socket);
        sageIngestor->setPort(port);
        sageIngestor->setHost(host);
    } else if (system.compare("cust_odbc_bulk") == 0) {
        //TESTS ON SQL SERVER SHOWED THIS IS VERY SLOW. BUT NO CLUE WHY, DID NOT BOTHER TO LOOK AT PROFILER YET
        sageIngestor->setSocket(socket);
        sageIngestor->setPort(port);
        sageIngestor->setHost(host);
    }
    
    // setup resume option, if desired
    sageIngestor->setResumeMode(resumeMode); 
    sageIngestor->setIsDryRun(isDryRun);
    sageIngestor->setAskUserToValidateRead(askUserToValidateRead); 
   
    cout << "now everything ready to ingest ..." << endl;
   
    //now ingest data after setup
    sageIngestor->setPerformanceMeter(outputFreq);	// after how many lines should I print the status?
    cout << "Go now!" << endl;
    sageIngestor->ingestData(bufferSize);  		// buffer size (in bytes??)
    
    delete thisSchemaMapper;
    delete thisSchema;
    //delete assertFac;
    //delete convFac;

    return 0;
}

