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

#include <Reader.h>
#include <string>
#include <fstream>
#include <stdio.h>
#include <assert.h>
#include <list>
#include <sstream>
#include <map>

#ifndef Sage_Sage_Reader_h
#define Sage_Sage_Reader_h

using namespace DBReader;
using namespace DBDataSchema;
using namespace std;
//using cout;
//using endl;


// structure for header
typedef struct {
    int Ntrees;
    int NtotGals;
    int *GalsPerTree;
} SageHeader;

namespace Sage {

//#pragma pack(push)  // push current alignment to stack; may not work with each and every compiler!
//#pragma pack(1)     // set alignment to 1 byte boundary
    // galaxy data structure
    typedef struct GalaxyData {
        int SnapNum;
        int Type;
        long GalaxyIndex;
        long CentralGalaxyIndex;
        long CtreesHaloID;
        int TreeIndex;
        long CtreesCentralID;
        int mergeType;
        int mergeIntoID;
        int mergeIntoSnapNum;
        float dT;
        float Pos[3];
        float Vel[3];
        float Spin[3];
        int Len;
        float Mvir;
        float CentralMvir;
        float Rvir;
        float Vvir;
        float Vmax;
        float VelDisp;
        float ColdGas;
        float StellarMass;
        float BulgeMass;
        float HotGas;
        float EjectedMass;
        float BlackHoleMass;
        float IntraClusterStars;
        float MetalsColdGas;
        float MetalsStellarMass;
        float MetalsBulgeMass;
        float MetalsHotGas;
        float MetalsEjectedMass;
        float MetalsIntraClusterStars;
        float SfrDisk;
        float SfrBulge;
        float SfrDiskZ;
        float SfrBulgeZ;
        float DiskRadius;
        float Cooling;
        float Heating;
        float QuasarModeBHaccretionMass;
        float TimeOfLastMajorMerger;
        float TimeOfLastMinorMerger;
        float OutflowRate;
        float MeanStarAge;
        float infallMvir;
        float infallVvir;
        float infallVmax;
    } GalaxyData;
//#pragma pack(pop)   // restore original alignment from stack

    class SageReader : public Reader {
    private:
        string fileName;
        string mapFile;

        ifstream fileStream;

        SageHeader header;

        long ioutput; // number of current output
        long numOutputs; // total number of outputs (one for reach redshift)
        long numDataSets; // number of DataSets (= row fields, = columns) in each output
        long nvalues; // values in one dataset (assume the same number for each dataset of the same output group (redshift))
        long blocksize; // number of elements in one read-block, should be small enough to fit (blocksize * number of datasets) into memory
        long maxRows; // max. number of rows per file, usually used for testing

        long totalRows; // total number of rows in data file

        long snapnumfactor;
        long rowfactor;
        float posfactor; // factor to multiply with coordinates, to get correct units (Mpc from kpc)

        vector<string> dataSetNames; // vector containing names of the HDF5 datasets
        map<string,int> dataSetMap;

        // improve performance by defining it here (instead of inside getItemInRow)
        string tmpStr;

        long currRow;
        long countInBlock;
        int countSnap;

        GalaxyData datarow; // stores one row of the read data

        float scale;
        float redshift;
        long dbId;
        long rockstarId;
        long depthFirstId;
        long forestId;
        long NInFile;
        int fileNum;
        int ix;
        int iy;
        int iz;
        long phkey;

        float h;
        int bswap;

    public:
        SageReader();
        SageReader(string newFileName, int bswap, int fileNum, int newBlocksize, long maxRows, vector<string> datafileFieldNames);
        // DBDataSchema::Schema*&
        ~SageReader();

        void openFile(string newFileName);

        void closeFile();

        long getMeta();

        int getNextRow();
        int readNextBlock(long blocksize);
        //long* readLongDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
        //int8_t* readTinyIntDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
    
        //double* readDoubleDataSet(const string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
        //float* readFloatDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
 
        //long getNumRowsInDataSet(string s);

        //vector<string> getDataSetNames();

        //void setCurrRow(long n);
        //long getCurrRow();
        //long getNumOutputs();
        GalaxyData byteswap_GalaxyData(GalaxyData *galdata, int bswap);

        int assignInt(int *n, char *memblock, int bswap);
        int assignLong(long int *n, char *memblock, int bswap);
        int assignFloat(float *n, char *memblock, int bswap);
        int swapInt(int i, int bswap);
        long swapLong(long l, int bswap);
        float swapFloat(float f, int bswap);
        
        bool getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result);

        bool getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result);

        void getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result);
    };
    
}

#endif
