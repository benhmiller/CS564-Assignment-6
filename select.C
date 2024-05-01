#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	Status status;
	AttrDesc projNamesDescs[projCnt];
	//convert attrInfo to attrDesc
	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projNamesDescs[i]);
		if (status != OK){
			return status;
		}
	}

	int reclen1 = 0;
	for(int i = 0; i < projCnt; i++){
		reclen1 += projNamesDescs[i].attrLen;
	}

	//get comparison information
	AttrDesc *attrDescArray = NULL;
	if(attr != NULL) {
		attrDescArray = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDescArray);
		if (status != OK) {
			return status;
		}
	}

	return ScanSelect(result, projCnt, projNamesDescs, attrDescArray, op, attrValue, reclen1);

}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	//create temporary record for output table
	char outputData[reclen];
    Record temporaryRecord;
    temporaryRecord.data = (void *) outputData;
    temporaryRecord.length = reclen;

	Status status;

	// change filter from char to integer and float values
	int value;
	float fvalue;


	// open "result" as an InsertFileScan object
	InsertFileScan resultRel(result, status);

	// check that both scan objects were made correctly
	if (status != OK) {
		return status;
	}

	//open current table (to be scanned) as a HeapFileScan object
	HeapFileScan curScan(string(projNames[0].relName), status); 

	// check that both scan objects were made correctly
	if (status != OK) {
		return status;
	}

	// check if an unconditional scan is required
	if(attrDesc == NULL) {
		if ((status = curScan.startScan(0, 0, STRING,  NULL, EQ)) != OK) {
			return status;
		}
	}

	// check attrType before scanning, then scan the current table
	else {
		switch(attrDesc->attrType) {
			case INTEGER:
				value = atoi(filter);
				status = curScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char *)&value, (Operator) op);
				break;
	
			case FLOAT:
				fvalue = atof(filter);
				status = curScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char *)&fvalue, (Operator) op);
				break;

			case STRING:
				status = curScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, (Operator) op);
				break;
		}
	
		if(status != OK) {
			return status;
		}
	}
	
	RID relRID;
	Record destRec;
	//copy found record to temporary record
	while (curScan.scanNext(relRID) == OK) {
  			status = curScan.getRecord(destRec);
			if (status != OK) {
				break;
			}
    
			int outputOffset = 0;
    		for(int i = 0; i < projCnt; i++) {
				memcpy((char *)outputData + outputOffset, (char *)destRec.data + projNames[i].attrOffset, projNames[i].attrLen);

				outputOffset += projNames[i].attrLen;
  			}

			RID outRid;
			status = resultRel.insertRecord(temporaryRecord, outRid);
			if(status != OK){
				return status;
			}

	}
	return OK;

}