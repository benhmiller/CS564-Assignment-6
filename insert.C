#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
					   const int attrCnt,
					   const attrInfo attrList[])
{

	// Obtain attribute descriptions from attribute catalog
	AttrDesc* attrDescArray;
	int actualAttrCount;
	attrCat->getRelInfo(relation, actualAttrCount, attrDescArray);
	Status status;

	// Check attribute count
	if (actualAttrCount != attrCnt) {
		return BADCATPARM;
	}

	// Obtain record length from attribute descriptions; simultaneous check for attribute values
	int reclen = 0; // Stores output record length
	for (int i = 0; i < attrCnt; i++) {
		// Check if insert attribute in list NULL
		if (attrList[i].attrValue == NULL) { // Null attribute value --> abort
			return BADCATPARM;
		}

		// Increase output record length
		reclen += attrDescArray[i].attrLen;
	}

	// Initialize New Record
	char outputData[reclen];
	Record outputRec;
	outputRec.data = (void *)outputData;
	outputRec.length = reclen;

	// Use offsets to construct new record in correct order
	int value = 0;
	float fValue = 0;
	int outputOffset = 0;

	for (int i = 0; i < attrCnt; i++) {
		bool attrFound = false;
		for (int j = 0; j < attrCnt; j++) { // Search for corresponding attribute in list
			if (strcmp(attrDescArray[i].attrName, attrList[j].attrName) == 0)
			{ // Located index of correctly ordered attribute
			attrFound = true;
				outputOffset = attrDescArray[i].attrOffset;
				switch (attrList[j].attrType)
				{
				case INTEGER:
					value = atoi((char *)attrList[j].attrValue);
					memcpy(outputData + outputOffset, (char *)attrList[j].attrValue, attrDescArray[i].attrLen);
					break;
				case FLOAT:
					fValue = atof((char *)attrList[j].attrValue);
					memcpy(outputData + outputOffset, (char *)attrList[j].attrValue, attrDescArray[i].attrLen);
					break;
				case STRING:
					memcpy((char*) outputData + outputOffset, (char *)attrList[j].attrValue, attrDescArray[i].attrLen);
					break;
				}
			}
		}
		if(!attrFound) { // Unable to locate inserting attribute in relation
			return UNIXERR;
		}
	}

	// Open the result table and insert the record
	InsertFileScan resultRel(relation, status);
	if (status != OK)
	{
		return status;
	}

	RID outRID;
	status = resultRel.insertRecord(outputRec, outRID);
	if (status != OK)
	{
		return status;
	}
	// Return successfully
	return OK;
}