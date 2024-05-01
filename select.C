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
	
	//get comparison information
	AttrDesc *attrDescWhere = NULL;
	int attrValueLen = 0;
	if(attr != NULL) {
		attrDescWhere = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDescWhere);
		attrValueLen = attrDescWhere->attrLen;
		if (status != OK) {
			return status;
		}
	}

	return ScanSelect(result, projCnt, projNamesDescs, attrDescWhere, op, attrValue, attrValueLen);

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

	RID rid;
	Status status;
	int intValue;
	float floatValue;

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
			case STRING:
				status = curScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, (Operator) op);
				break;
	
			case INTEGER:
		 		intValue = atoi(filter);
				status = curScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char *)&intValue, (Operator) op);
				break;
	
			case FLOAT:
				floatValue = atof(filter);
				status = curScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char *)&floatValue, (Operator) op);
				break;
		}
	
		if(status != OK) {
			return status;
		}
	}
	
	// cout << "Dumped at copying record" << endl;
	RID relRID;
	//copy found record to temporary record
	while (curScan.scanNext(relRID) == OK) {
			Record destRec;
  			status = curScan.getRecord(destRec);
			if (status != OK) {
				break;
			}
      	
    		// attrInfo attrList[projCnt];
			// int tmpInt;
			// float tmpFloat;
			int outputOffset = 0;

    		for(int i = 0; i < projCnt; i++) {
				// AttrDesc attrDesc = projNames[i];

				// switch(projNames[i].attrType){
				// 	case INTEGER: 
				// 		tmpInt = atoi((char *)destRec.data + attrDesc.attrOffset);
				// 		memcpy(outputData + outputOffset, &tmpInt, attrDesc.attrLen);
				// 		break;
				// 	case FLOAT:
				// 		tmpFloat = atof((char *)destRec.data + attrDesc.attrOffset);
				// 		memcpy(outputData + outputOffset, &tmpFloat, attrDesc.attrLen);
				// 		break;
				// 	case STRING:
				// 		memcpy((char *)outputData + outputOffset, (char *)destRec.data + attrDesc.attrOffset, attrDesc.attrLen);
				// 		break;
					

				// 	case STRING:
				// 		memcpy((char *)temporaryRecord.data + outputOffset, (char *)destRec.data + projNames[i].attrOffset, projNames[i].attrLen);
				// 		break;
				// 	case INTEGER:
				// 		printf("%d\n", (char*)destRec.data + projNames[i].attrOffset);
				// 		memcpy((char *)temporaryRecord.data + outputOffset, (int *)destRec.data + projNames[i].attrOffset, projNames[i].attrLen);
				// 		// sprintf((char *)(temporaryRecord.data + outputOffset), "%d", tmpInt);
				// 		break;
				// 	case FLOAT:
				// 		printf("%f\n", (char*)destRec.data + projNames[i].attrOffset);
				// 		memcpy((char *)temporaryRecord.data + outputOffset, (float *)destRec.data + projNames[i].attrOffset, projNames[i].attrLen);
				// 		// sprintf((char *)(temporaryRecord.data + outputOffset), "%f", tmpFloat);
				// 		break;
				// }


				memcpy((char *)outputData + outputOffset, (char *)destRec.data + projNames[i].attrOffset, projNames[i].attrLen);

				
				outputOffset += projNames[i].attrLen;

  			}

			RID outRid;
			status = resultRel.insertRecord(destRec, outRid);
			if(status != OK){
				return status;
			}

	}
	return OK;

}