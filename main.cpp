#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <cstring>

void changeAttName(const char *rel, const char *old, const char *newname)
{
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
  HeadInfo attrCatHeader;
  HeadInfo attrCatHeader_store;
  attrCatBuffer.getHeader(&attrCatHeader);

  attrCatHeader_store = attrCatHeader;

  while (1)
  {
    for (int i = 0; i < attrCatHeader.numEntries; i++)
    {
      // declare attrCatRecord and load the attribute catalog entry into it
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBuffer.getRecord(attrCatRecord, i);

      if (!strcmp(rel, attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal) && !strcmp(old, attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal))
      {
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
        std::cout << " Success\n\n";
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newname);
        std::cout << attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal << "\n";
        attrCatBuffer.setRecord(attrCatRecord, i);

        return;
      }
    }
    if (attrCatHeader.rblock == -1)
    {
      std::cout << "Not found\n\n";
      return;
    }
    attrCatBuffer = RecBuffer(attrCatHeader.rblock);
    attrCatBuffer.getHeader(&attrCatHeader);
  }
}

void printAllTable()
{
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
  RecBuffer attrCatBuffer_store = attrCatBuffer;

  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;

  HeadInfo attrCatHeader_store;

  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  int total_rel_count = relCatHeader.numEntries;

  int count;
  int total_count;

  attrCatHeader_store = attrCatHeader;

  for (int i = 0; i < total_rel_count; i++)
  {

    Attribute relCatRecord[RELCAT_NO_ATTRS]; // will store the record from the relation catalog

    relCatBuffer.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    count = 0;
    total_count = relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    while (count < total_count)
    {
      for (int j = 0; j < attrCatHeader.numEntries; j++)
      {

        // declare attrCatRecord and load the attribute catalog entry into it
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRecord, j);

        if (strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal) == 0)
        {
          count++;
          const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
          printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
        }
      }

      if (attrCatHeader.rblock == -1 || count == total_count)
      {

        break;
      }

      attrCatBuffer = RecBuffer(attrCatHeader.rblock);
      attrCatBuffer.getHeader(&attrCatHeader);
    }
    attrCatHeader = attrCatHeader_store;
    attrCatBuffer = attrCatBuffer_store;
    printf("\n");
  }
}

int main(int argc, char *argv[])
{
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;


  /*

  RelCatEntry *relCatBuf = (RelCatEntry *)malloc(sizeof(RelCatEntry));
  AttrCatEntry *attrCatBuf = (AttrCatEntry *)malloc(sizeof(AttrCatEntry));

  for (int i = 0; i <= 2; i++)
  {

    // get the relation catalog entry using
    RelCacheTable::getRelCatEntry(i, relCatBuf);
    printf("Relation: %s\n", relCatBuf->relName);

    for (int j = 0; j < relCatBuf->numAttrs; j++)
    {
      // get the attribute catalog entry for (rel-id i, attribute offset j)
      // in attrCatEntry using AttrCacheTable::getAttrCatEntry()
      AttrCacheTable::getAttrCatEntry(i, j, attrCatBuf);

      printf("  %s: %s\n", attrCatBuf->attrName, attrCatBuf->attrType == 0 ? "NUM" : "STR");
    }
  }
  */

  /*unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, 7000);
  char message[] = "hello";
  memcpy(buffer + 20, message, 6);
  Disk::writeBlock(buffer, 7000);

  unsigned char buffer2[BLOCK_SIZE];
  char message2[6];
  Disk::readBlock(buffer2, 7000);
  memcpy(message2, buffer2 + 20, 6);
  std::cout << message2;


  unsigned char buff[BLOCK_SIZE];
  Disk::readBlock(buff,0);

  for(int i=0;i<10;i++){
    std::cout<<"disk "<<int(buff[i])<<"\n";
  }

  return 0;*/

  // printAllTable();
  // changeAttName("Students", "Class", "Batch");
  // printAllTable();

 

  return FrontendInterface::handleFrontend(argc, argv);
}
