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

# ******* Script Starts ********************************
try:
	# Initiate
	logging.info("Begining Parcel Atrributes python script")
	#killObject(outRecords)
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
##	logging.info("Create a new table in Geodatabase")
##	arcpy.CreateTable_management(outPath, outName)
##	field1 = {'Name': 'ASSESSOR_N', 'Type': 'TEXT', 'Size': 11}
##	field2 = {'Name': 'ATTRIB_TEM', 'Type': 'TEXT', 'Size': 20}
##	field3 = {'Name': 'ATTRIB_NAM', 'Type': 'TEXT', 'Size': 30}
##	field4 = {'Name': 'ATTRIB_VAL', 'Type': 'TEXT', 'Size': 50}
##	field5 = {'Name': 'SEQ_SOURCE_NUM', 'Type': 'SHORT'}
##	arcpy.AddField_management(outRecords, field1['Name'], field1['Type'], field_length=field1['Size'])
##	arcpy.AddField_management(outRecords, field2['Name'], field2['Type'], field_length=field2['Size'])
##	arcpy.AddField_management(outRecords, field3['Name'], field3['Type'], field_length=field3['Size'])
##	arcpy.AddField_management(outRecords, field4['Name'], field4['Type'], field_length=field4['Size'])
##	arcpy.AddField_management(outRecords, field5['Name'], field5['Type'])
	# Truncate table already made
	arcpy.TruncateTable_management(outRecords)
	# Create a list for all records
	myList = []

	# Populate the geodatabase table and the SQL table
	logging.info("Populating databases with information")
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
	con = pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'
	 r'SERVER=172.20.10.141;'
	 r'DATABASE=Accela;'
	 r'UID=Accela;'
	 r'PWD=Pw4accela'
	 )
	cursort = con.cursor()
	cursort.execute('TRUNCATE TABLE dbo.Parcel_Attr;')
	con.commit()
	rocks = arcpy.InsertCursor(outRecords)
	for iList in myList:
		rock = rocks.newRow()
		rock.setValue("ASSESSOR_N", iList[0])
		rock.setValue("ATTRIB_NAM", iList[1])
		rock.setValue("ATTRIB_VAL", iList[2])
		rocks.insertRow(rock)
		SQLCommand = ("INSERT INTO dbo.Parcel_Attr" "(source_seq_nbr, L1_attrib_Temp_Name, L1_Parcel_Nbr, L1_Attrib_Name, L1_Attrib_value) " "VALUES (?,?,?,?,?)")
		myValues = [1, 'PARCEL_ATTRIBUTES', iList[0], iList[1], iList[2]]
		cursort.execute(SQLCommand,myValues)
		con.commit()
	del rocks
	del rock
	con.close()
	del cursort
	expression = "1"
	arcpy.CalculateField_management(outRecords, "SEQ_SOURCE_NUM", expression, "PYTHON_9.3")
	expression = "'PARCEL_ATTRIBUTES'"
	arcpy.CalculateField_management(outRecords, "ATTRIB_TEM", expression, "PYTHON_9.3")
	# closing up program
	killObject(outRecords2)
	logging.info("End of program; Program ran correctly as written")
	logging.shutdown()


except Exception as e:

   logging.error(traceback.format_exc())
   logging.shutdown()

