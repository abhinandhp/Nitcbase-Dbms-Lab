#include "OpenRelTable.h"
#include "stdlib.h"
#include <cstring>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable()
{

    // initialize relCache and attrCache with nullptr
    for (int i = 0; i < MAX_OPEN; ++i)
    {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        tableMetaInfo[i].free = true;
    }

    /************ Setting up Relation Cache entries ************/
    // (we need to populate relation cache with entries for the relation catalog
    //  and attribute catalog.)

    /**** setting up Relation Catalog relation in the Relation Cache Table****/
    RecBuffer relCatBlock(RELCAT_BLOCK);

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

    // allocate this on the heap because we want it to persist outside this function
    RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

    /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

    relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

    // set up the relation cache entry for the attribute catalog similarly
    // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT

    // struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = ATTRCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

    // set the value at RelCacheTable::relCache[ATTRCAT_RELID]
    RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;

    /**** setting up Students relation in the Relation Cache Table ****/

    // relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT + 1);

    // set up the relation cache entry for the students similarly
    // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT+1

    // struct RelCacheEntry relCacheEntry;
    /*
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = ATTRCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT + 1;

    // set the value at RelCacheTable::relCache[ATTRCAT_RELID]
    RelCacheTable::relCache[ATTRCAT_RELID + 1] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[ATTRCAT_RELID + 1]) = relCacheEntry;
    */

    /************ Setting up Attribute cache entries ************/
    // (we need to populate attribute cache with entries for the relation catalog
    //  and attribute catalog.)

    /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
    RecBuffer attrCatBlock(ATTRCAT_BLOCK);

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

    // iterate through all the attributes of the relation catalog and create a linked
    // list of AttrCacheEntry (slots 0 to 5)
    // for each of the entries, set
    //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    //    attrCacheEntry.recId.slot = i   (0 to 5)
    //    and attrCacheEntry.next appropriately
    // NOTE: allocate each entry dynamically using malloc
    AttrCacheEntry *head = NULL;
    AttrCacheEntry *attrCacheEntry = NULL;

    head = attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    for (int i = 0; i < 6; i++)
    {

        attrCatBlock.getRecord(attrCatRecord, i);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
        attrCacheEntry->recId.block = ATTRCAT_BLOCK;
        attrCacheEntry->recId.slot = i;
        if (i < 5)
        {
            attrCacheEntry->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
            attrCacheEntry = attrCacheEntry->next;
        }
    }

    // set the next field in the last entry to nullptr
    attrCacheEntry->next = nullptr;

    AttrCacheTable::attrCache[RELCAT_RELID] = head;

    /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

    // set up the attributes of the attribute cache similarly.
    // read slots 6-11 from attrCatBlock and initialise recId appropriately

    head = attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    for (int i = 6; i < 12; i++)
    {

        attrCatBlock.getRecord(attrCatRecord, i);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
        attrCacheEntry->recId.block = ATTRCAT_BLOCK;
        attrCacheEntry->recId.slot = i;
        if (i < 11)
        {
            attrCacheEntry->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
            attrCacheEntry = attrCacheEntry->next;
        }
    }
    attrCacheEntry->next = nullptr;

    // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
    AttrCacheTable::attrCache[ATTRCAT_RELID] = head;

    /**** setting up Students relation in the Attribute Cache Table ****/

    // set up the attributes of the attribute cache similarly.
    // read slots 12 to 12+#attrs-1 from Students and initialise recId appropriately

    /*
        head = attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        int numberOfAttributes = RelCacheTable::relCache[2]->relCatEntry.numAttrs;
        for (int i = 12; i < numberOfAttributes + 12; i++)
        {

            attrCatBlock.getRecord(attrCatRecord, i);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
            attrCacheEntry->recId.block = ATTRCAT_BLOCK;
            attrCacheEntry->recId.slot = i;
            if (i < 11 + numberOfAttributes)
            {
                attrCacheEntry->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
                attrCacheEntry = attrCacheEntry->next;
            }
        }
        attrCacheEntry->next = nullptr;

        // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
        AttrCacheTable::attrCache[ATTRCAT_RELID + 1] = head;
        */

    /************ Setting up tableMetaInfo entries ************/

    // in the tableMetaInfo array
    //   set free = false for RELCAT_RELID and ATTRCAT_RELID
    tableMetaInfo[0].free = false;
    tableMetaInfo[1].free = false;

    //   set relname for RELCAT_RELID and ATTRCAT_RELID
    strcpy(tableMetaInfo[0].relName, "RELATIONCAT");
    strcpy(tableMetaInfo[1].relName, "ATTRIBUTECAT");
}

/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE])
{

    /* traverse through the tableMetaInfo array,
   find the entry in the Open Relation Table corresponding to relName.*/

    for (int i = 0; i < MAX_OPEN; ++i)
    {
        if (strcmp(tableMetaInfo[i].relName, relName) == 0 && tableMetaInfo[i].free == false)
        {
            return i;
        }
    }

    return E_RELNOTOPEN;

    // if found return the relation id, else indicate that the relation do not
    // have an entry in the Open Relation Table.
}

OpenRelTable::~OpenRelTable()
{
    // free all the memory that you allocated in the constructor

    // close all open relations (from rel-id = 2 onwards. Why?)
    for (int i = 2; i < MAX_OPEN; ++i)
    {
        if (!tableMetaInfo[i].free)
        {
            OpenRelTable::closeRel(i); // we will implement this function later
        }
    }

    // free the memory allocated for rel-id 0 and 1 in the caches
}

int OpenRelTable::getFreeOpenRelTableEntry()
{

    /* traverse through the tableMetaInfo array,
      find a free entry in the Open Relation Table.*/
    for (int i = 2; i < MAX_OPEN; ++i)
    {
        if (tableMetaInfo[i].free)
        {
            return i;
        }
    }
    return E_CACHEFULL;
    // if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::openRel(char relName[ATTR_SIZE])
{

    int getrelId = getRelId(relName);
    if (getrelId >= 0)
    {
        // (checked using OpenRelTable::getRelId())

        return getrelId;
    }

    /* find a free slot in the Open Relation Table
       using OpenRelTable::getFreeOpenRelTableEntry(). */
    int relId = getFreeOpenRelTableEntry();

    if (relId == E_CACHEFULL)
    {
        return E_CACHEFULL;
    }

    // let relId be used to store the free slot.

    /****** Setting up Relation Cache entry for the relation ******/

    /* search for the entry with relation name, relName, in the Relation Catalog using
        BlockAccess::linearSearch().
        Care should be taken to reset the searchIndex of the relation RELCAT_RELID
        before calling linearSearch().*/

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute attrVal;
    strcpy(attrVal.sVal, relName);
    // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
    RecId relcatRecId;

    relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, attrVal, EQ);

    if (relcatRecId.block == -1 && relcatRecId.slot == -1)
    {
        // (the relation is not found in the Relation Catalog.)
        return E_RELNOTEXIST;
    }

    /* read the record entry corresponding to relcatRecId and create a relCacheEntry
        on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
        update the recId field of this Relation Cache entry to relcatRecId.
        use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
      NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
    */
    RecBuffer recBlock(relcatRecId.block);

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    recBlock.getRecord(relCatRecord, relcatRecId.slot);

    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = relcatRecId.block;
    relCacheEntry.recId.slot = relcatRecId.slot;

    // allocate this on the heap because we want it to persist outside this function
    RelCacheTable::relCache[relId] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[relId]) = relCacheEntry;

    /****** Setting up Attribute Cache entry for the relation ******/

    // let listHead be used to hold the head of the linked list of attrCache entries.
    AttrCacheEntry *listHead = nullptr;

    /*iterate over all the entries in the Attribute Catalog corresponding to each
    attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
    care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
    corresponding to Attribute Catalog before the first call to linearSearch().*/

    /* let attrcatRecId store a valid record id an entry of the relation, relName,
    in the Attribute Catalog.*/

    /* read the record entry corresponding to attrcatRecId and create an
    Attribute Cache entry on it using RecBuffer::getRecord() and
    AttrCacheTable::recordToAttrCatEntry().
    update the recId field of this Attribute Cache entry to attrcatRecId.
    add the Attribute Cache entry to the linked list of listHead .*/
    // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    AttrCacheEntry *ptr = nullptr;
    RecId attrcatRecId;

    int numattr = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
    for (int i = 0; i < numattr; i++)
    {
        if (i == 0)
        {
            listHead = ptr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        }
        else
        {
            ptr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
            ptr = ptr->next;
        }
    }
    ptr->next = nullptr;
    ptr = listHead;

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    for (int i = 0; i < numattr; i++)
    {
        attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, RELCAT_ATTR_RELNAME, attrVal, EQ);
        RecBuffer attrCatBlock(attrcatRecId.block);

        attrCatBlock.getRecord(attrCatRecord, attrcatRecId.slot);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(ptr->attrCatEntry));
        ptr->recId.block = attrcatRecId.block;
        ptr->recId.slot = attrcatRecId.slot;
        ptr = ptr->next;
    }

    // set the relIdth entry of the AttrCacheTable to listHead.
    AttrCacheTable::attrCache[relId] = listHead;

    /****** Setting up metadata in the Open Relation Table for the relation******/

    // update the relIdth entry of the tableMetaInfo with free as false and
    // relName as the input.

    tableMetaInfo[relId].free = false;
    strcpy(tableMetaInfo[relId].relName, relName);

    return relId;
}

int OpenRelTable::closeRel(int relId)
{
    if (relId == 0 || relId == 1)
    {
        return E_NOTPERMITTED;
    }

    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (tableMetaInfo[relId].free)
    {
        return E_RELNOTOPEN;
    }

    // free the memory allocated in the relation and attribute caches which was
    // allocated in the OpenRelTable::openRel() function
    free(RelCacheTable::relCache[relId]);
    AttrCacheEntry *ptr, *next;
    ptr = AttrCacheTable::attrCache[relId];
    while (ptr)
    {
        next = ptr->next;
        free(ptr);
        ptr = next;
    }

    // update `tableMetaInfo` to set `relId` as a free slot
    // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
    tableMetaInfo[relId].free = true;
    RelCacheTable::relCache[relId] = nullptr;
    AttrCacheTable::attrCache[relId] = nullptr;

    return SUCCESS;
}
