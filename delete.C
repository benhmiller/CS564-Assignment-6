#include "catalog.h"
#include "query.h"
#include <string>

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
// This function will delete all tuples in relation satisfying the predicate specified 
//   by attrName, op, and the constant attrValue. type denotes the type of the attribute. 
// You can locate all the qualifying tuples using a filtered HeapFileScan.

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{

	Status status;
	AttrDesc attrDesc;

	// get AttrDesc
	if(attrName.empty()){
		// Do nothing
	}
	else{
		status = attrCat->getInfo(relation, attrName, attrDesc);
		if (status != OK){ return status; }
	}

	// Create a HeapFileScan object
	HeapFileScan predScan(relation, status);
	if (status != OK){ return status; }

	// do startScan()
	if(attrName.empty()){
		//if attrName is NULL, set startScan’s offset and length to 0, type to string, filter to NULL
		cout << "Doing default scan" << endl;
		status = predScan.startScan(0, 0, STRING, NULL, op);
	}
	else{
		cout << "Doing filtered scan" << endl;
		// cout << "\tattrOffset: " << attrDesc.attrOffset << endl;
		// cout << "\tattrLen: " << attrDesc.attrLen << endl;
		// cout << "\ttype: " << type << endl;
		// cout << "\tattrValue: " << attrValue << endl;
		// cout << "\top: " << op << endl;
		status = predScan.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, attrValue, op);
	}
	if (status != OK){ return status; }

	// scan through all the records that match the predicate, and delete them
	RID predRID;
	Record predRec;
	while (predScan.scanNext(predRID) == OK){
		status = predScan.getRecord(predRec);
        ASSERT(status == OK);
		cout << "Record match: " << predRec.data << endl;

		predScan.deleteRecord();
		predScan.markDirty();
	}

	return OK;
}


