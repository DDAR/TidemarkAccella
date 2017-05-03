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

def getCorVar(corV):
	if corV == None:
		return "NULL"
	else:
		if corV.isdigit():
			return int(corV)
		else:
			return "NULL"

# ******* Script Starts ********************************
try:
	# Initiate
	logging.info("Begining Parcel Atrributes python script")
	killObject(outRecords)
	killObject(outRecords2)
	killObject(layer)
	logging.debug("Killed old records")
	#---Set Evnironment Settings
	arcpy.env.workspace = r"R:\Geodatabase\Taxlots\SupportFiles"
	arcpy.env.qualifiedFieldNames = False
	#--- Copy geodbase to working copy
	arcpy.CopyFeatures_management(geoBase, outRecords2)
	# Strip uneeded fields out of features
	fieldsToKeep = ["ASSESSOR_N","CNY_ZONE", "CNYZONE", "COMM_DIST", "FIRE_DIST", "FIRM_PNL_", "IRRG_DIST", "JURISDICT", "MEASURE", "NEW_CONST", "PLAN_DES", "SCHOOL", "SIZE", "UGA", "USE_CODE"]
	fieldObjList = arcpy.ListFields(outRecords2)
	fieldNameList = []
	for field in fieldObjList:
		if not field.required:
			fieldNameList.append(field.name)
	finalDelList = [x for x in fieldNameList if x not in fieldsToKeep]
	arcpy.DeleteField_management(outRecords2, finalDelList)
	# Delete Identical records
	logging.info("Removing Duplicate Records")
	arcpy.DeleteIdentical_management(outRecords2, "ASSESSOR_N")
	# Create Table
	logging.info("Create a new table in Geodatabase")
	arcpy.CreateTable_management(outPath, outName)
	field1 = {'Name': 'ASSESSOR_N', 'Type': 'TEXT', 'Size': 11}
	field2 = {'Name': 'ATTRIB_TEM', 'Type': 'TEXT', 'Size': 20}
	field3 = {'Name': 'ATTRIB_NAM', 'Type': 'TEXT', 'Size': 30}
	field4 = {'Name': 'ATTRIB_VAL', 'Type': 'TEXT', 'Size': 50}
	field5 = {'Name': 'SEQ_SOURCE_NUM', 'Type': 'SHORT'}
	arcpy.AddField_management(outRecords, field1['Name'], field1['Type'], field_length=field1['Size'])
	arcpy.AddField_management(outRecords, field2['Name'], field2['Type'], field_length=field2['Size'])
	arcpy.AddField_management(outRecords, field3['Name'], field3['Type'], field_length=field3['Size'])
	arcpy.AddField_management(outRecords, field4['Name'], field4['Type'], field_length=field4['Size'])
	arcpy.AddField_management(outRecords, field5['Name'], field5['Type'])
	# Populate the geodatabase table and the SQL table
	logging.info("Populating databases with information")
	attTem = 'PARCEL_ATTRIBUTES'
	seQ = 1
	cursor = arcpy.SearchCursor(outRecords2)
	rocks = arcpy.InsertCursor(outRecords)
	for row in cursor:
		assNum = row.getValue("ASSESSOR_N")
		for a in range(1,15):
			attNam = fieldsToKeep[a]
			attVal = row.getValue(attNam)
			#strMine = str(assNum) + ": " + str(attNam) + ": " + str(attVal)
			#logging.debug(strMine)
			rock = rocks.newRow()
			rock.setValue("ASSESOR_N", assNum)
			rock.setValue("ATTRIB_TEM", attTem)
			rock.setValue("ATTRIB_NAM", attNam)
			rock.setValue("ATTRIB_VAL", attVal)
			rock.setValue("SEQ_SOURCE_NUM", seQ)
			rocks.insert(rock)





	del cursor
	del row
	del rocks
	del rock
	logging.info("End of program; Program ran correctly as written")
	logging.shutdown()


except Exception as e:

   logging.error(traceback.format_exc())
   logging.shutdown()

