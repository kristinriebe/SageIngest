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

#include <iostream>

#include "Sage_SchemaMapper.h"
#include <SchemaItem.h>
#include <DataObjDesc.h>
#include <DType.h>
#include <DBType.h>
#include <stdlib.h>

using namespace std;
using namespace DBDataSchema;

namespace Sage {
    
    SageSchemaMapper::SageSchemaMapper() {
        
    }

    SageSchemaMapper::SageSchemaMapper(DBAsserter::AsserterFactory * newAssertFac, DBConverter::ConverterFactory * newConvFac) {
        assertFac = newAssertFac;
        convFac = newConvFac;
    }

    SageSchemaMapper::~SageSchemaMapper() {
        
    }

    DataField::DataField() {
        name = "";
        type = "unknown";
    }

    DataField::DataField(string newName) {
        name = newName;
        type = "unknown";
    }
    DataField::DataField(string newName, string newType) {
        name = newName;
        type = newType;
    }

    vector<string> SageSchemaMapper::getFieldNames() {
        // get fieldnames from Sage-Reader?
        // or reuse the fields defined here in Sage-Reader

        DataField dataField;
        vector<string> fieldNames;

        datafileFields.clear();
        databaseFields.clear();

        dataField.name = "dbId";
        dataField.type = "BIGINT";
        databaseFields.push_back(dataField);

        dataField.name = "snapnum";
        dataField.type = "SMALLINT";
        databaseFields.push_back(dataField);

        dataField.name = "redshift";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "rockstarId";
        dataField.type = "BIGINT";
        databaseFields.push_back(dataField);

        dataField.name = "GalaxyID";
        dataField.type = "BIGINT";
        databaseFields.push_back(dataField);

        dataField.name = "HostHaloID";
        dataField.type = "BIGINT";
        databaseFields.push_back(dataField);

        dataField.name = "MainHaloID";
        dataField.type = "BIGINT";
        databaseFields.push_back(dataField);

        dataField.name = "GalaxyType";
        dataField.type = "TINYINT";
        databaseFields.push_back(dataField);

        dataField.name = "HaloMass";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "Vmax";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "x";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "y";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "z";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "vx";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "vy";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "vz";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "MstarSpheroid";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "MstarDisk";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "McoldDisk";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "Mhot";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "Mbh";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "SFRspheroid";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "SFRdisk";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "SFR";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "ZgasSpheroid";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "ZgasDisk";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "MZhotHalo";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "MZstarSpheroid";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "MZstarDisk";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "MeanAgeStars";
        dataField.type = "FLOAT";
        databaseFields.push_back(dataField);

        dataField.name = "NInFile";
        dataField.type = "BIGINT";
        databaseFields.push_back(dataField);

        dataField.name = "fileNum";
        dataField.type = "INTEGER";
        databaseFields.push_back(dataField);

        dataField.name = "ix";
        dataField.type = "INTEGER";
        databaseFields.push_back(dataField);

        dataField.name = "iy";
        dataField.type = "INTEGER";
        databaseFields.push_back(dataField);

        dataField.name = "iz";
        dataField.type = "INTEGER";
        databaseFields.push_back(dataField);

        dataField.name = "phkey";
        dataField.type = "BIGINT";
        databaseFields.push_back(dataField);

        // copy names into a simple string vector for returning it
        for (int j=0; j<databaseFields.size(); j++) {
            databaseFieldNames.push_back(databaseFields[j].name);
        }

        // print for checking
        cout << "DataFields (database): " << databaseFields.size() << endl;
        for (int j=0; j<databaseFields.size(); j++) {
            cout << "  Fieldnames " << j << ":" << databaseFields[j].name << endl;
            cout << "  Fieldtypes " << j << ":" << databaseFields[j].type << endl;
        }

        datafileFields = databaseFields; 

        return databaseFieldNames;
    }

    DBDataSchema::Schema * SageSchemaMapper::generateSchema(string dbName, string tblName) {
        DBDataSchema::Schema * returnSchema = new Schema();

        string datafileFieldName;
        string databaseFieldName;

        string dbtype_str;
        string dtype_str;

        DType dtype;
        DBType dbtype;

        //set database and table name
        returnSchema->setDbName(dbName);
        returnSchema->setTableName(tblName);

        //setup schema items and add them to the schema
        for (int j=0; j<databaseFields.size(); j++) {

            datafileFieldName = datafileFields[j].name;
            databaseFieldName = databaseFields[j].name;

            dtype_str  = databaseFields[j].type;    // data file type -- actually I am using databasetype here for convenience!
            if (dtype_str == "DOUBLE") {
                dtype_str = "REAL";
            }
            //dtype = DBDataSchema::convDType(dtype_str);
            dtype = getDType(dtype_str);  //because I am using the same here

            dbtype_str = databaseFields[j].type;    // database field type
            if (dbtype_str == "DOUBLE") {
                dbtype_str = "REAL";
            }
            dbtype = getDBType(dbtype_str); 

            //first create the data object describing the input data:
            DataObjDesc* colObj = new DataObjDesc();
            colObj->setDataObjName(datafileFieldName);	// column name (data file)
            colObj->setDataObjDType(dtype);	// data file type
            colObj->setIsConstItem(false, false); // not a constant
            colObj->setIsHeaderItem(false);	// not a header item
        
            //then describe the SchemaItem which represents the data on the server side
            SchemaItem* schemaItem = new SchemaItem();
            schemaItem->setColumnName(databaseFieldName);	// field name in Database
            schemaItem->setColumnDBType(dbtype);		// database field type
            schemaItem->setDataDesc(colObj);		// link to data object

            //add schema item to the schema
            returnSchema->addItemToSchema(schemaItem);
        }

        return returnSchema;
    }

    DBType SageSchemaMapper::getDBType(string thisDBType) {
        
        if (thisDBType == "CHAR") {
            return DBT_CHAR;
        }
        if (thisDBType == "BIT") {
            return DBT_BIT;
        }
        if (thisDBType == "BIGINT") {
            return DBT_BIGINT;
        }
        if (thisDBType == "MEDIUMINT") {
            return DBT_MEDIUMINT;
        }
        if (thisDBType == "INTEGER") {
            return DBT_INTEGER;
        }
        if (thisDBType == "SMALLINT") {
            return DBT_SMALLINT;
        }
        if (thisDBType == "TINYINT") {
            return DBT_TINYINT;
        }
        if (thisDBType == "FLOAT") {
            return DBT_FLOAT;
        }
        if (thisDBType == "REAL") {
            return DBT_REAL;
        }
        if (thisDBType == "DATE") {
            return DBT_DATE;
        }
        if (thisDBType == "TIME") {
            return DBT_TIME;
        }
        if (thisDBType == "ANY") {
            return DBT_ANY;
        }
        if (thisDBType == "UBIGINT") {
            return DBT_UBIGINT;
        }
        if (thisDBType == "UMEDIUMINT") {
            return DBT_UMEDIUMINT;
        }
        if (thisDBType == "UINTEGER") {
            return DBT_UINTEGER;
        }
        if (thisDBType == "USMALLINT") {
            return DBT_USMALLINT;
        }
        if (thisDBType == "UTINYINT") {
            return DBT_UTINYINT;
        }
        if (thisDBType == "UFLOAT") {
            return DBT_UFLOAT;
        }
        if (thisDBType == "UREAL") {
            return DBT_UREAL;
        }
    
        // error: type not known
        return (DBType) 0;    
    }

    DType SageSchemaMapper::getDType(string thisDBType) {
        
        if (thisDBType == "CHAR") {
            return DT_STRING;
        }
        if (thisDBType == "BIGINT") {
            return DT_INT8;
        }
        //if (thisDBType == "MEDIUMINT") {
        //    return DBT_MEDIUMINT;
        //}
        if (thisDBType == "INTEGER") {
            return DT_INT4;
        }
        if (thisDBType == "SMALLINT") {
            return DT_INT2;
        }
        if (thisDBType == "TINYINT") {
            return DT_INT1;
        }
        if (thisDBType == "FLOAT") {
            return DT_REAL4;
        }
        if (thisDBType == "REAL") {
            return DT_REAL8;
        }
        //if (thisDBType == "DATE") {
        //    return DBT_DATE;
        //}
        //if (thisDBType == "TIME") {
        //    return DBT_TIME;
        //}
        //if (thisDBType == "ANY") {
        //    return DBT_ANY;
        //}
        if (thisDBType == "UBIGINT") {
            return DT_UINT8;
        }
        //if (thisDBType == "UMEDIUMINT") {
        //    return DBT_UMEDIUMINT;
        //}
        if (thisDBType == "UINTEGER") {
            return DT_UINT4;
        }
        if (thisDBType == "USMALLINT") {
            return DT_UINT2;
        }
        if (thisDBType == "UTINYINT") {
            return DT_UINT1;
        }
        //if (thisDBType == "UFLOAT") {
        //    return DBT_UFLOAT;
        //}
        //if (thisDBType == "UREAL") {
        //    return DBT_UREAL;
        //}
    
        // error: type not known
        return (DType) 0;    
    }

}
