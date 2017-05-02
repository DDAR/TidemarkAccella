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
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_AttrTest"
outRecords2 = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcels_AddressTest2"
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

try:
	# Initiate
	killObject(outRecords)
	killObject(outRecords2)
	killObject(layer3)
	logging.debug("Killed old records")
	killObject(layer)
	#---Set Evnironment Settings
	arcpy.env.workspace = r"R:\Geodatabase\Taxlots\SupportFiles"
	arcpy.env.qualifiedFieldNames = False

	# Create a layer to join records to
	arcpy.MakeFeatureLayer_management (taxSpatialRecord, layer, whereClauseSEl)
	logging.debug("New Feature Layer made")

	# Join Records to the properties table
	arcpy.AddJoin_management(layer, "ASSESSOR_N", propTable, "ASSESSOR_N")

	# Create the feature class in the geodatabase and drop fields
	arcpy.CopyFeatures_management(layer, outRecords)
	dropFields = ["AREA", "PERIMETER", "CNTYPARC_", "CNTYPARC_I", "RTS", "PARC", "ACRES", "OBJECTID_1", "ASSESSOR_N_1", "TCA", "TAX_YEAR", "USE_CODE", "LOCATED_ON", "MKT_LAND", "MKT_IMPVT", "NEW_CONST", "CU_LAND",
	 "CU_IMPVT", "SIZE", "MEASURE", "CU_DATE", "CU_VALUE", "CYCLE", "NBHD", "INS_DATE",  "INS_NUM", "CUR_CYCLE", "HOUSE_NO",]
	arcpy.DeleteField_management(outRecords, dropFields)

	# Delete Identical records
	arcpy.DeleteIdentical_management(outRecords, "ASSESSOR_N")
	logging.debug("Delete Identical Records")

	# Add fields
	arcpy.AddField_management(outRecords, "SOURCE_SEQ_NBR", "SHORT", 2)
	arcpy.AddField_management(outRecords, "SERV_PROV_CODE", "TEXT", 15)
	arcpy.AddField_management(outRecords, "HOUSE", "LONG")
	arcpy.AddField_management(outRecords, "UNIT", "TEXT", 10)
	arcpy.AddField_management(outRecords, "STR_SUFFIX", "TEXT", 30)
	arcpy.AddField_management(outRecords, "SITUS_STATE", "TEXT", 30)
	logging.debug("Add new Fields")

	# Parse Address
	address_fields = "SITUS_ADDR"
	locator_style = "US Address - Single House"
	add_fields = "HouseNum;PreDir;PreType;StreetName;SufType;SufDir"
	arcpy.StandardizeAddresses_geocoding(outRecords, address_fields, locator_style, add_fields, outRecords2)
	killObject(outRecords)
	logging.debug("Parcing addresses")

	# Calculate Added Fields
	logging.debug("Calculating new fields")
	# -- calculate SOURCE_SEQ_NBR
	expression = "1"
	arcpy.CalculateField_management(outRecords2, "SOURCE_SEQ_NBR", expression, "PYTHON_9.3")
	# -- calculate SERV_PROV_CODE
	expression = "'YAKIMACO'"
	arcpy.CalculateField_management(outRecords2, "SERV_PROV_CODE", expression, "PYTHON_9.3")
	# -- calculate SITUS_STATE
	expression = "'WA'"
	arcpy.CalculateField_management(outRecords2, "SITUS_STATE", expression, "PYTHON_9.3")
	# -- calculate HOUSE
	cursor = arcpy.UpdateCursor(outRecords2)
	for row in cursor:
		#logging.debug("Curosr Return")
		pickel = row.getValue(field)
		r = getCorVar(pickel)
		if type(r) is int:
			row.setValue(field2, r)
		cursor.updateRow(row)
	del cursor
	del row

	# Spatial Join Building Footprints
	spatialJoins()

	# Use Building footprints to populate standard fields
	# -- Calculate SITUS_ADDR with Address
	logging.debug("Calculating Building Footprint attributes")
	#field = 'Address'
	#arcpy.CalculateField_management(outRecords2, 'SITUS_ADDR', '!Address!', "PYTHON_9.3")
	cursor = arcpy.UpdateCursor(outRecords2)
	for row in cursor:
		address = row.getValue('Address')
##		logging.debug(str(address))
		hn = row.getValue('House_Number')
		pd = row.getValue('PREDIR')
		sn = row.getValue('STREETNAME')
		st = row.getValue('STREETTYPE')
		ci = row.getValue('City')
		zc = row.getValue('ZipCode')
		if address != None:
			row.setValue('HOUSE', hn)
			row.setValue('SITUS_ADDR', address)
			row.setValue('ADDR_PD', pd)
			row.setValue('ADDR_SN', SQLEsc2(sn))
			row.setValue('ADDR_ST', st)
			row.setValue('SITUS_CITY', SQLEsc2(ci))
			row.setValue('SITUS_ZIP', zc)
##			logging.debug(str(row.getValue('SITUS_ADDR')))
##			logging.debug("--------------")
		cursor.updateRow(row)
	del cursor
	del row

	# Remove null or empty records of ADDR_SN and SITUS_CITY
	up_curs = arcpy.UpdateCursor(outRecords2, "", "", "", "")
	for row in up_curs:
		rg = row.getValue('ADDR_SN')
		rt = row.getValue('SITUS_CITY')
		val = str(rg).strip()
		val2 = str(rt).strip()
		if len(val) == 0:
			row.setNull('ADDR_SN')
		if len(val2) == 0:
			row.setNull('SITUS_CITY')
		up_curs.updateRow(row)
	del up_curs
	del row
	layer2 = 'OutTemp'
	killObject(layer2)
	killObject(layer3)
	expression = '"SITUS_CITY" IS NULL'
	expression2 = '"ADDR_SN" IS NULL'
	arcpy.MakeFeatureLayer_management(outRecords2, layer2)
	arcpy.SelectLayerByAttribute_management(layer2, "NEW_SELECTION", expression)
	if int(arcpy.GetCount_management(layer2).getOutput(0)) > 0:
		arcpy.DeleteFeatures_management(layer2)
	arcpy.SelectLayerByAttribute_management(layer2, "NEW_SELECTION", expression2)
	if int(arcpy.GetCount_management(layer2).getOutput(0)) > 0:
		arcpy.DeleteFeatures_management(layer2)
	arcpy.CopyFeatures_management(layer2, layer3)

	# Load data into SQL database
	con = pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'
	 r'SERVER=172.20.10.141;'
	 r'DATABASE=Accela;'
	 r'UID=Accela;'
	 r'PWD=Pw4accela'
	 )
	cursort = con.cursor()
	cursort.execute('TRUNCATE TABLE dbo.Parcel_address;')
	con.commit()
	outCursor = arcpy.SearchCursor(layer3)
	for row in outCursor:
		xService = row.getValue("SERV_PROV_CODE")
		xSource = row.getValue("SOURCE_SEQ_NBR")
		xParcid = row.getValue("ASSESSOR_N")
		xHouse = row.getValue("HOUSE")
		#logging.info(str(SQLEsc2(xHouse)))
		xUnit = row.getValue("UNIT")
		xStdirec = row.getValue("ADDR_PD")
		xStname = row.getValue("ADDR_SN")
		xStsuffix = row.getValue("ADDR_ST")
		xStSufxDir = row.getValue("STR_SUFFIX")
		xCity = row.getValue("SITUS_CITY")
		xState = row.getValue("SITUS_STATE")
		xZip = row.getValue("SITUS_ZIP")
		xAddress = row.getValue("SITUS_ADDR")

		#insertStr = """INSERT INTO dbo.Parcel_address(SERV_PROV_CODE, SOURCE_SEQ_NBR, L1_PARCEL_NBR, L1_HSE_NBR_START, L1_UNIT_START, L1_STR_DIR, L1_STR_NAME, L1_STR_SUFFIX, L1_STR_SUFFIX_DIR, L1_SITUS_CITY, L1_SITUS_STATE, L1_SITUS_ZIP, L1_ADDRESS1) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12})""".format(SQLEsc2(xService), SQLEsc2(xSource), SQLEsc2(xParcid), SQLEsc2(xHouse), SQLEsc2(xUnit), SQLEsc2(xStdirec), SQLEsc2(xStname), SQLEsc(xStsuffix), SQLEsc2(xStSufxDir), SQLEsc2(xCity), SQLEsc2(xState), SQLEsc2(xZip), SQLEsc2(xAddress))
		#insertStr = """INSERT INTO dbo.Parcel_address(Serv_prov_code) VALUES ('YAKIMACO')"""
		SQLCommand = ("INSERT INTO dbo.Parcel_address" "(SOURCE_SEQ_NBR, Serv_prov_code, L1_PARCEL_NBR, L1_HSE_NBR_START, L1_UNIT_START, L1_STR_DIR, L1_STR_NAME, L1_STR_SUFFIX, L1_STR_SUFFIX_DIR, L1_SITUS_CITY, L1_SITUS_STATE, L1_SITUS_ZIP, L1_ADDRESS1) " "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
		myValues = [SQLEsc2(xSource), SQLEsc2(xService), SQLEsc2(xParcid), SQLEsc2(xHouse), SQLEsc2(xUnit), SQLEsc2(xStdirec), SQLEsc2(xStname), SQLEsc2(xStsuffix), SQLEsc2(xStSufxDir), SQLEsc2(xCity), SQLEsc2(xState), SQLEsc2(xZip), SQLEsc2(xAddress)]
 		#cursort.execute(insertStr)
		cursort.execute(SQLCommand,myValues)
		con.commit()

	con.close()
	del cursort
	killObject(outRecords2)
	killObject(outRecords)
	killObject(layer)
	killObject(layer2)
	logging.info("End of program; Program ran correctly as written")
	logging.shutdown()


except Exception as e:

   logging.error(traceback.format_exc())
   logging.shutdown()

