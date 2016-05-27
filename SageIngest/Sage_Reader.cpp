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
#include <stdio.h>
#include <stdlib.h>
#include <math.h>   // sqrt, pow
#include "sageingest_error.h"
#include <list>
//#include <boost/filesystem.hpp>
//#include <boost/serialization/string.hpp> // needed on erebos for conversion from boost-path to string()
#include <boost/regex.hpp> // for string regex match/replace to remove redshift from dataSetNames

#include "Sage_Reader.h"

//using namespace boost::filesystem;

//#include <boost/chrono.hpp>
//#include <cmath>
#include <boost/date_time/posix_time/posix_time.hpp>


namespace Sage {
    SageReader::SageReader() {

        currRow = 0;
    }

    SageReader::SageReader(string newFileName, int newBswap, float newH, int newFileNum, int newBlocksize, long newMaxRows, vector<string>datafileFieldNames) {

        fileName = newFileName;
        
        // just let the user provide a file number for this snapnum/redshift
        fileNum = newFileNum;
        bswap = newBswap;

        maxRows = newMaxRows;

        currRow = 0;
        countInBlock = 0;   // counts rows in each block

        blocksize = newBlocksize; // size of block in rows, i.e. row number in each block

        // factors for constructing dbId, could/should be read from user input, actually
        snapnumfactor = 1000; // must be less than max(fileNum from user)! -> 1000 is exactly the number.
        rowfactor = 10000000;

        dbId = 0;
        redshift = -1;  // fill in later via DB

        h = newH; //for MDPL2, Planck cosm.: 0.6777

        rockstarId = 0;
        depthFirstId = 0;
        forestId = 0;


        openFile(newFileName);

        totalRows = getMeta();

        if (maxRows == -1) {
            maxRows = totalRows;
        }

        if (maxRows > totalRows) {
            printf("WARNING: total number of rows is %ld, but %ld rows were requested. Setting maxRows to %ld.\n",
                totalRows, maxRows, totalRows);
            maxRows = totalRows;
        }

        // allocate memory for datablock
        // (only needed once; mem. size won't change after this point, only at
        // the end there may be less data to be read, which is no problem)
        datarows = (GalaxyData *) malloc(blocksize*sizeof(GalaxyData));

        //cout << "size of dataSetMap: " << dataSetMap.size() << endl;
    }


    SageReader::~SageReader() {
        closeFile();
        // delete datablock

        if (datarows) {
            free(datarows);
        }

    }
    
    void SageReader::openFile(string newFileName) {
        // open the binary file

        if (fileStream.is_open())
            fileStream.close();

        // open binary file
        fileStream.open(newFileName.c_str(), ios::in | ios::binary);
        
        if (!(fileStream.is_open())) {
            SageIngest_error("SageReader: Error in opening file.\n");
        }
        
        fileName = newFileName;
    }
    
    void SageReader::closeFile() {
        if (fileStream.is_open())
            fileStream.close();
    }

    long SageReader::getMeta() {

        assert(fileStream.is_open());

        char memchunk[4];
        int *GalsPerTree;
        long mRows;

        fileStream.read((char *) &header.Ntrees, sizeof(header.Ntrees));
        header.Ntrees = swapInt(header.Ntrees, bswap);

        fileStream.read((char *) &header.NtotGals, sizeof(header.NtotGals));
        header.NtotGals = swapInt(header.NtotGals, bswap);
        
        // also read num gal. per tree:
        GalsPerTree = (int *) malloc(header.Ntrees*sizeof(int));
        //cout << "size gals: " << sizeof(GalsPerTree) << endl;
        fileStream.read((char *) GalsPerTree, header.Ntrees*sizeof(int));

        if (bswap) {
            for (int i=0; i<header.Ntrees; i++) {
               GalsPerTree[i] = swapInt(GalsPerTree[i], bswap);
            }
        }

        mRows = header.NtotGals;

        // check:
        printf("Ntrees, NtotGals: %d %d\n", header.Ntrees, header.NtotGals);
        printf("Galaxies in this tree: 0: %d, 1: %d, 2: %d, 3: %d, 500: %d, 1000: %d\n", 
            GalsPerTree[0], GalsPerTree[1], GalsPerTree[2],GalsPerTree[3], GalsPerTree[500], GalsPerTree[1000]);

        return mRows;
    }

//#pragma pack(push)    
//#pragma pack(1)        
    int SageReader::readNextBlock(long blocksize) {
        assert(fileStream.is_open());
 
        //performance output stuff
        boost::posix_time::ptime startTime;
        boost::posix_time::ptime endTime;

        // make sure that we won't exceed the max. number
        // of rows/total rows in this file:
        // TODO!
        blocksize = min(blocksize, maxRows-currRow);
        if (currRow >= maxRows) {
            // already reached end of file, no more data available
            cout << "End of dataset reached. Nothing more to read. Done" << endl;
            return 0;
        }

        // read a whole block of data at once, 
        // more efficient than just reading line by line
        startTime = boost::posix_time::microsec_clock::universal_time();

        if (!fileStream.read((char *) datarows, blocksize*sizeof(GalaxyData)));

        endTime = boost::posix_time::microsec_clock::universal_time();
        printf("Time for reading (%ld rows): %lld ms\n", blocksize, (long long int) (endTime-startTime).total_milliseconds());

        return blocksize;
    }


    int SageReader::getNextRow() {
        assert(fileStream.is_open());
        
        // read one line from already read datablock (see readNextBlock)
        // readNextBlock returns number of read values
        if (currRow == 0) {
            blocksize = readNextBlock(blocksize);
            countInBlock = 0;
        } else if (countInBlock == blocksize-1) {
            // end of block reached, read the next block
            blocksize = readNextBlock(blocksize);
            //cout << "nvalues in getNextRow: " << nvalues << endl;
            countInBlock = 0;
        } else {
            //cout << "blocksize: " << blocksize << endl;
            countInBlock++;
        }

        if (blocksize <= 0) {
            // might happen if end of file reached in readNextBlock (if currRow >= maxRows in min() statement)
            return 0;
        }

        // if not using readNextBlock:
        // fileStream.read((char *) &datarow, sizeof(GalaxyData));

        datarow = datarows[countInBlock];
        if (bswap) {
            datarow = byteswap_GalaxyData(&datarow, bswap);
        }

        currRow++; // global counter for all rows

        return 1;
    }
//#pragma pack(pop) 
    bool SageReader::getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result) {
        
        bool isNull;

        isNull = false;

        //reroute constant items:
        if(thisItem->getIsConstItem() == true) {
            getConstItem(thisItem, result);
            isNull = false;
        } else if (thisItem->getIsHeaderItem() == true) {
            printf("We never told you to read headers...\n");
            exit(EXIT_FAILURE);
        } else {
            isNull = getDataItem(thisItem, result);
        }
        
        // assertions and conversions could be applied here
        // but do not need them now.

        // return value: if true: just NULL is written, result is ignored.
        // if false: the value in result is used.
        return isNull;
    }
    
    bool SageReader::getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result) {

        //check if this is "Col1" etc. and if yes, assign corresponding value
        //the variables are declared already in Pmss_Reader.h 
        //and the values were read in getNextRow()
        bool isNull;

        isNull = false;

        if(thisItem->getDataObjName().compare("dbId") == 0) {
            *(long*)(result) = (datarow.SnapNum * snapnumfactor + fileNum) * rowfactor + currRow;
        } else if(thisItem->getDataObjName().compare("snapnum") == 0) {
            *(short*)(result) = datarow.SnapNum;
        } else if(thisItem->getDataObjName().compare("redshift") == 0) {
            *(float*)(result) = redshift;
        } else if(thisItem->getDataObjName().compare("rockstarId") == 0) {
            *(long*)(result) = datarow.CtreesHaloID; // should be the same as HostHaloId
        } else if(thisItem->getDataObjName().compare("depthFirstId") == 0) {
            *(long*)(result) = depthFirstId;
        } else if(thisItem->getDataObjName().compare("forestId") == 0) {
            *(long*)(result) = forestId;
        } else if(thisItem->getDataObjName().compare("GalaxyID") == 0) {
            *(long*)(result) = datarow.GalaxyIndex;
        } else if(thisItem->getDataObjName().compare("HostHaloID") == 0) {
            *(long*)(result) = datarow.CtreesHaloID;
        } else if (thisItem->getDataObjName().compare("MainHaloID") == 0) {
            *(long*)(result) = datarow.CtreesCentralID;
        } else if (thisItem->getDataObjName().compare("GalaxyType") == 0) {
            *(short*)(result) = datarow.Type;
        } else if (thisItem->getDataObjName().compare("HaloMass") == 0) {
            *(float*)(result) = datarow.Mvir*1.e10;
        } else if (thisItem->getDataObjName().compare("Vmax") == 0) {
            *(float*)(result) = datarow.Vmax;
        } else if (thisItem->getDataObjName().compare("x") == 0) {
            *(float*)(result) = datarow.Pos[0];
        } else if (thisItem->getDataObjName().compare("y") == 0) {
            *(float*)(result) = datarow.Pos[1];
        } else if (thisItem->getDataObjName().compare("z") == 0) {
            *(float*)(result) = datarow.Pos[2];
        } else if (thisItem->getDataObjName().compare("vx") == 0) {
            *(float*)(result) = datarow.Vel[0];
        } else if (thisItem->getDataObjName().compare("vy") == 0) {
            *(float*)(result) = datarow.Vel[1];
        } else if (thisItem->getDataObjName().compare("vz") == 0) {
            *(float*)(result) = datarow.Vel[2];
        } else if (thisItem->getDataObjName().compare("MstarSpheroid") == 0) {
            *(float*)(result) = datarow.BulgeMass*1.e10;
        } else if (thisItem->getDataObjName().compare("MstarDisk") == 0) {
            *(float*)(result) = (datarow.StellarMass - datarow.BulgeMass)*1.e10;
        } else if (thisItem->getDataObjName().compare("McoldDisk") == 0) {
            *(float*)(result) = datarow.ColdGas*1.e10;
        } else if (thisItem->getDataObjName().compare("Mhot") == 0) {
            *(float*)(result) = datarow.HotGas*1.e10;
        } else if (thisItem->getDataObjName().compare("Mbh") == 0) {
            *(float*)(result) = datarow.BlackHoleMass*1.e10;
        } else if (thisItem->getDataObjName().compare("SFRspheroid") == 0) {
            *(float*)(result) = datarow.SfrBulge*h*1.e9;
        } else if (thisItem->getDataObjName().compare("SFRdisk") == 0) {
            *(float*)(result) = datarow.SfrDisk*h*1.e9;
        } else if (thisItem->getDataObjName().compare("SFR") == 0) {
            *(float*)(result) = (datarow.SfrBulge + datarow.SfrDisk)*h*1.e9;
        } else if (thisItem->getDataObjName().compare("ZgasSpheroid") == 0) {
            *(float*)(result) = datarow.SfrBulgeZ;
        } else if (thisItem->getDataObjName().compare("ZgasDisk") == 0) {
            *(float*)(result) = datarow.MetalsColdGas/datarow.ColdGas;
        } else if (thisItem->getDataObjName().compare("MZhotHalo") == 0) {
            *(float*)(result) = datarow.MetalsHotGas*1.e10;
        } else if (thisItem->getDataObjName().compare("MZstarSpheroid") == 0) {
            *(float*)(result) = datarow.MetalsBulgeMass*1.e10;
        } else if (thisItem->getDataObjName().compare("MZstarDisk") == 0) {
            *(float*)(result) = (datarow.MetalsStellarMass - datarow.MetalsBulgeMass)*1.e10;
        } else if (thisItem->getDataObjName().compare("MeanAgeStars") == 0) {
            *(float*)(result) = datarow.MeanStarAge/h/1.e3;
        } else if (thisItem->getDataObjName().compare("NInFile") == 0) {
            *(long*)(result) = currRow;
        } else if (thisItem->getDataObjName().compare("fileNum") == 0) {
            *(int*)(result) = datarow.SnapNum * snapnumfactor + fileNum;
        } else if (thisItem->getDataObjName().compare("ix") == 0) {
            *(int*)(result) = 0; // if box size and ngrid was provided, we could calculate it here directly
        } else if (thisItem->getDataObjName().compare("iy") == 0) {
            *(int*)(result) = 0;
        } else if (thisItem->getDataObjName().compare("iz") == 0) {
            *(int*)(result) = 0;
        } else if (thisItem->getDataObjName().compare("phkey") == 0) {
            phkey = 0;
            *(int*)(result) = phkey;
            // better: let DBIngestor insert Null at this column
            // => need to return 1, so that Null will be written.
            isNull = true;
        } else {
            printf("Something went wrong in getDataItem(), field %s not found ...\n", thisItem->getDataObjName().c_str());
            exit(EXIT_FAILURE);
        }

        return isNull;

    }

    void SageReader::getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result) {
        memcpy(result, thisItem->getConstData(), DBDataSchema::getByteLenOfDType(thisItem->getDataObjDType()));
    }


    // write part from memoryblock to integer; byteswap, if necessary (TODO: use global 'swap' or locally submit?)
    int SageReader::assignInt(int *n, char *memblock, int bswap) {
        
        unsigned char *cptr,tmp;

        if (sizeof(int) != 4) {
            fprintf(stderr,"assignInt: sizeof(int)=%ld and not 4. Can't handle that.\n",sizeof(int));
            return 0;
        }

        if (!memcpy(n,  memblock, sizeof(int))) {
            fprintf(stderr,"Error: Encountered end of memory block or other trouble when reading the memory block.\n");
            return 0;
        }

        if (bswap) {
            cptr = (unsigned char *) n;
            tmp     = cptr[0];
            cptr[0] = cptr[3];    
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }

        return 1;
    }

    // write part from memoryblock to long; byteswap, if necessary
    int SageReader::assignLong(long *n, char *memblock, int bswap) {
        
        unsigned char *cptr,tmp;

        if (sizeof(long) != 8) {
            fprintf(stderr,"assignLong: sizeof(long)=%ld and not 8. Can't handle that.\n",sizeof(long));
            return 0;
        }

        if (!memcpy(n,  memblock, sizeof(long))) {
            fprintf(stderr,"Error: Encountered end of memory block or other trouble when reading the memory block.\n");
            return 0;
        }

        if (bswap) {
            cptr = (unsigned char *) n;
            tmp     = cptr[0];
            cptr[0] = cptr[7];
            cptr[7] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[6];
            cptr[6] = tmp;
            tmp     = cptr[2];
            cptr[2] = cptr[5];
            cptr[5] = tmp;
            tmp     = cptr[3];
            cptr[3] = cptr[4];
            cptr[4] = tmp;
        }

        return 1;
    }

    // write part from memoryblock to float; byteswap, if necessary
    int SageReader::assignFloat(float *n, char *memblock, int bswap) {
        
        unsigned char *cptr,tmp;
        
        if (sizeof(float) != 4) {
            fprintf(stderr,"assignFloat: sizeof(float)=%ld and not 4. Can't handle that.\n",sizeof(float));
            return 0;
        }
        
        if (!memcpy(n, memblock, sizeof(float))) {
            printf("Error: Encountered end of memory block or other trouble when reading the memory block.\n");
            return 0;
        }
        
        if (bswap) {
            cptr = (unsigned char *) n;
            tmp     = cptr[0];
            cptr[0] = cptr[3];    
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }
        return 1;
    }

    int SageReader::swapInt(int i, int bswap) {
        unsigned char *cptr,tmp;
        
        if ( (int)sizeof(int) != 4 ) {
            fprintf( stderr,"Swap int: sizeof(int)=%d and not 4\n", (int)sizeof(int) );
            exit(0);
        }
        
        if (bswap) {
            cptr = (unsigned char *) &i;
            tmp     = cptr[0];
            cptr[0] = cptr[3];
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }

        return i;
    }


    long SageReader::swapLong(long i, int bswap) {
        unsigned char *cptr,tmp;
        
        if ( (int)sizeof(long) != 8 ) {
            fprintf( stderr,"Swap long: sizeof(long)=%d and not 8\n", (int)sizeof(long) );
            exit(0);
        }
        
        if (bswap) {
            cptr = (unsigned char *) &i;
            tmp     = cptr[0];
            cptr[0] = cptr[7];
            cptr[7] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[6];
            cptr[6] = tmp;
            tmp     = cptr[2];
            cptr[2] = cptr[5];
            cptr[5] = tmp;
            tmp     = cptr[3];
            cptr[3] = cptr[4];
            cptr[4] = tmp;
        }

        return i;
    }

    float SageReader::swapFloat(float f, int bswap) {
        unsigned char *cptr,tmp;
        
        if (sizeof(float) != 4) {
        fprintf(stderr,"Swap float: sizeof(float)=%d and not 4\n",(int)sizeof(float));
        exit(0);
        }
         
        if (bswap) {
            cptr = (unsigned char *)&f;
            tmp     = cptr[0];
            cptr[0] = cptr[3];
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }

        return f;
    }


    GalaxyData SageReader::byteswap_GalaxyData(GalaxyData *galdata, int bswap) {

        GalaxyData newdata;
        newdata.SnapNum = swapInt(galdata->SnapNum, bswap);
        newdata.Type = swapInt(galdata->Type, bswap);
        newdata.GalaxyIndex = swapLong(galdata->GalaxyIndex, bswap);
        newdata.CentralGalaxyIndex = swapLong(galdata->CentralGalaxyIndex, bswap);
        newdata.CtreesHaloID = swapLong(galdata->CtreesHaloID, bswap);
        newdata.TreeIndex = swapInt(galdata->TreeIndex, bswap);
        newdata.CtreesCentralID = swapLong(galdata->CtreesCentralID, bswap);
        newdata.mergeType = swapInt(galdata->mergeType, bswap);
        newdata.mergeIntoID = swapInt(galdata->mergeIntoID, bswap);
        newdata.mergeIntoSnapNum = swapInt(galdata->mergeIntoSnapNum, bswap);
        newdata.dT = swapFloat(galdata->dT, bswap);
        for (int i=0; i<3; i++) {
            newdata.Pos[i] = swapFloat(galdata->Pos[i], bswap);
            newdata.Vel[i] = swapFloat(galdata->Vel[i], bswap);
            newdata.Spin[i] = swapFloat(galdata->Spin[i], bswap);
        }
        newdata.Len  = swapInt(galdata->Len, bswap);
        newdata.Mvir = swapFloat(galdata->Mvir, bswap);
        newdata.CentralMvir = swapFloat(galdata->CentralMvir, bswap);
        newdata.Rvir = swapFloat(galdata->Rvir, bswap);
        newdata.Vvir = swapFloat(galdata->Vvir, bswap);
        newdata.Vmax = swapFloat(galdata->Vmax, bswap);
        newdata.VelDisp = swapFloat(galdata->VelDisp, bswap);
        newdata.ColdGas = swapFloat(galdata->ColdGas, bswap);
        newdata.StellarMass = swapFloat(galdata->StellarMass, bswap);
        newdata.BulgeMass = swapFloat(galdata->BulgeMass, bswap);
        newdata.HotGas = swapFloat(galdata->HotGas, bswap);
        newdata.EjectedMass = swapFloat(galdata->EjectedMass, bswap);
        newdata.BlackHoleMass = swapFloat(galdata->BlackHoleMass, bswap);
        newdata.IntraClusterStars = swapFloat(galdata->IntraClusterStars, bswap);
        newdata.MetalsColdGas = swapFloat(galdata->MetalsColdGas, bswap);
        newdata.MetalsStellarMass = swapFloat(galdata->MetalsStellarMass, bswap);
        newdata.MetalsBulgeMass = swapFloat(galdata->MetalsBulgeMass, bswap);
        newdata.MetalsHotGas = swapFloat(galdata->MetalsHotGas, bswap);
        newdata.MetalsEjectedMass = swapFloat(galdata->MetalsEjectedMass, bswap);
        newdata.MetalsIntraClusterStars = swapFloat(galdata->MetalsIntraClusterStars, bswap);
        newdata.SfrDisk = swapFloat(galdata->SfrDisk, bswap);
        newdata.SfrBulge = swapFloat(galdata->SfrBulge, bswap);
        newdata.SfrDiskZ = swapFloat(galdata->SfrDiskZ, bswap);
        newdata.SfrBulgeZ = swapFloat(galdata->SfrBulgeZ, bswap);
        newdata.DiskRadius = swapFloat(galdata->DiskRadius, bswap);
        newdata.Cooling = swapFloat(galdata->Cooling, bswap);
        newdata.Heating = swapFloat(galdata->Heating, bswap);
        newdata.QuasarModeBHaccretionMass = swapFloat(galdata->QuasarModeBHaccretionMass, bswap);
        newdata.TimeOfLastMajorMerger = swapFloat(galdata->TimeOfLastMajorMerger, bswap);
        newdata.TimeOfLastMinorMerger = swapFloat(galdata->TimeOfLastMinorMerger, bswap);
        newdata.OutflowRate = swapFloat(galdata->OutflowRate, bswap);
        newdata.MeanStarAge = swapFloat(galdata->MeanStarAge, bswap);
        newdata.infallMvir = swapFloat(galdata->infallMvir, bswap);
        newdata.infallVvir = swapFloat(galdata->infallVvir, bswap);
        newdata.infallVmax = swapFloat(galdata->infallVmax, bswap);
        
        return newdata;
    }

}

