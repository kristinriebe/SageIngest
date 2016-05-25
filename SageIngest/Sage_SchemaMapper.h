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

#include <SchemaDataMapGenerator.h>
#include <DBType.h>
#include <AsserterFactory.h>
#include <ConverterFactory.h>
#include <string>
#include <stdio.h>

#include <sstream>
#include <vector>
#include <iostream>
#include <fstream>
#include <map>

#ifndef Sage_Sage_SchemaMapper_h
#define Sage_Sage_SchemaMapper_h

using namespace DBDataSchema;

namespace Sage {
    class DataField {
        public:
            std::string name;
            std::string type;

            DataField();
            DataField(std::string name);
            DataField(std::string name, std::string type);
    };


    class SageSchemaMapper : public SchemaDataMapGenerator  {
        
    private:
        DBAsserter::AsserterFactory * assertFac;
        DBConverter::ConverterFactory * convFac;

        std::vector<std::string> datafileFieldNames;
        std::vector<std::string> databaseFieldNames;

        //std::vector<DataField> datafileFields, databaseFields;

    public:
        SageSchemaMapper();

        SageSchemaMapper(DBAsserter::AsserterFactory * newAssertFac, DBConverter::ConverterFactory * newConvFac);

        ~SageSchemaMapper();

        std::vector<std::string> getFieldNames();

        DBType getDBType(std::string thisDBType);
        DType  getDType(std::string thisDBType);

        DBDataSchema::Schema * generateSchema(std::string dbName, std::string tblName);

        std::vector<DataField> datafileFields, databaseFields; // make it public, so I can access it from the reader as well
    };

}


#endif
