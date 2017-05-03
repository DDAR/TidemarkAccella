#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      donnaa
#
# Created:     03/05/2017
# Copyright:   (c) donnaa 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# Name:		ParcelAttributes.py
# Purpose:	 To create a table from parcels and parcel tables to be inputed
#			  into the Tidemark-Accella system
#
# Author:	  donnaa
#
# Created:	 02/05/2017
# Copyright:   (c) donnaa 2017
# Licence:	 <your licence>
#-------------------------------------------------------------------------------
import os
import string
import sys
import arcpy
import time
import pyodbc
import logging
import traceback
from arcpy import env
from datetime import date
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf8')

# Variables ---------------------------
logging.basicConfig(filename=r'd:\data\temp\logFile2.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
outPath = r"R:\Geodatabase\Taxlots\Accela.gdb"
outName = "Parcel_AttrTest"
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_AttrTest"
outRecords2 = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcels_Att2"
geoBase = r"R:\Geodatabase\Taxlots\Taxlots.gdb\geodbase"
layer = "manageAddtaxlots"
fieldsToKeep = ["ASSESSOR_N","CNY_ZONE", "CNYZONE", "COMM_DIST", "FIRE_DIST", "FIRM_PNL_", "IRRG_DIST", "JURISDICT", "MEASURE", "NEW_CONST", "PLAN_DES", "SCHOOL", "SIZE", "UGA", "USE_CODE"]

# Methods   ---------------------------

def killObject( object ):
	if arcpy.Exists(object):
		arcpy.Delete_management(object)

def SQLEsc(s):
	if s == None:
		return "NULL"
	else:
		return "'"+string.replace(s, "'", "''")+"'"

def SQLEsc2(s):
	if s == None:
		return None
	else:
		return s
try:
    arcpy.TruncateTable_management(outRecords)
    myList = []

    rows = arcpy.SearchCursor(outRecords2)
    for row in rows:
        assNum = row.getValue("ASSESSOR_N")
        for a in range(1,15):
            attNam = fieldsToKeep[a]
            attVal = row.getValue(attNam)
            if attVal is None:
                pass
            else:
                if isinstance(attVal, basestring):
                    val = str(attVal).replace(" ", "")
                    if len(val) > 0:
                        fList = [assNum, attNam, attVal]
                        myList.append(fList)
                elif isinstance(attVal, int):
                    if attVal > 0:
                        fList = [assNum, attNam, attVal]
                        myList.append(fList)
    del rows
    del row
    rocks = arcpy.InsertCursor(outRecords)
    for iList in myList:
        rock = rocks.newRow()
        rock.setValue("ASSESSOR_N", iList[0])
        rock.setValue("ATTRIB_NAM", iList[1])
        rock.setValue("ATTRIB_VAL", iList[2])
        rocks.insertRow(rock)

    del rocks
    del rock




except Exception as e:

   logging.error(traceback.format_exc())
   logging.shutdown()