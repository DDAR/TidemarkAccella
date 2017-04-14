#-------------------------------------------------------------------------------
# Name:		ParcelBaseTable.py
# Purpose:	 To create a table from parcels and parcel tables to be inputed
#			  into the Tidemark-Accella system
#
# Author:	  donnaa
#
# Created:	 13/04/2017
# Copyright:   (c) donnaa 2017
# Licence:	 <your licence>
#-------------------------------------------------------------------------------
import os
import string
import sys
import arcpy
import time
import pyodbc
from arcpy import env
from datetime import date
from datetime import datetime


# Variables ---------------------------
taxSpatialRecord = r"M:\Geodatabase\Taxlots\Taxlots.gdb\parcels"
partyTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Party"
legalTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Legal"
propTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Property"
commishRec = r"M:\Geodatabase\boundary\districts.gdb\voting\commissioner"
censusRec = r"M:\Geodatabase\census\Census 2010 Geography.gdb\Tracts"
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTest"
outRecCom = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCom"
#outRecCen = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCen"
remFields = ["AREA", "PERIMETER", "SYMBOL", "ImageURL", "BioURL", "NAME"]
dropfieldsINSJ = ["Join_Count", "TARGET_FID"]
layer = "managetaxlots"
whereClauseSEl = '"PARC" > 10000 AND "PARC" < 50000'
field = "RTS"
field2 = "RANGE"
field3 = "TOWNSHIP"
field4 = "SECTION"
field5 = "SEC"

# Methods   ---------------------------

def killObject( object ):
	if arcpy.Exists(object):
		arcpy.Delete_management(object)

def spatialJoins():
    try:
        killObject(outRecCom)
        #killObject(outRecCen)
        fieldmappings = arcpy.FieldMappings()
        fieldmappings.addTable(outRecords)
        fieldmappings.addTable(commishRec)
        for rr in remFields:
            x = fieldmappings.findFieldMapIndex(rr)
            fieldmappings.removeFieldMap(x)

        arcpy.SpatialJoin_analysis(outRecords, commishRec, outRecCom, "#", "#", fieldmappings)
        for rs in dropfieldsINSJ:
            arcpy.DeleteField_management(outRecCom, rs)

        killObject(outRecords)

    #   *******************************************************************
        fieldmappings2 = arcpy.FieldMappings()
        fieldmappings2.addTable(outRecCom)
        fieldmappings2.addTable(censusRec)
        for rs in remFields2:
            x = fieldmappings2.findFieldMapIndex(rs)
            fieldmappings2.removeFieldMap(x)

        arcpy.SpatialJoin_analysis(outRecCom, censusRec, outRecords, "#", "#", fieldmappings2)
        for rs in dropfieldsINSJ:
            arcpy.DeleteField_management(outRecords, rs)

        killObject(outRecCom)


    except IOError as e:
        print 'Exception error is: %s' % e


def main():
	try:
		# Initiate
		killObject(outRecords)
		killObject(layer)
		#---Set Evnironment Settings
		arcpy.env.workspace = r"R:\Geodatabase\Taxlots\SupportFiles"
		arcpy.env.qualifiedFieldNames = False

		# Create a layer to join records to
		arcpy.MakeFeatureLayer_management (taxSpatialRecord, layer, whereClauseSEl)

		# Join Records to the properties table
		arcpy.AddJoin_management(layer, "ASSESSOR_N", propTable, "ASSESSOR_N")

		# join Records to the legal table
		arcpy.AddJoin_management(layer, "ASSESSOR_N", legalTable, "ASSESSOR_N")

		# Create the feature class in the geodatabase and drop fields
		arcpy.CopyFeatures_management(layer, outRecords)
		dropFields = ["AREA", "PERIMETER", "CNTYPARC_", "CNTYPARC_I", "ACRES", "OBJECTID_1", "ASSESSOR_N_1", "TCA", "TAX_YEAR", "USE_CODE", "LOCATED_ON", "NEW_CONST", "CU_LAND",
		 "CU_IMPVT", "MEASURE", "SITUS_ADDR", "SITUS_ZIP", "SITUS_CITY", "CU_DATE", "CU_VALUE", "CYCLE", "NBHD", "INS_DATE", "CUR_CYCLE", "HOUSE_NO", "OBJECTID_12", "ASSESSOR_N_12",
		 "LINE_NR", "SEG_CHILD_"]
		arcpy.DeleteField_management(outRecords, dropFields)

		# Delete Identical records
		arcpy.DeleteIdentical_management(outRecords, "ASSESSOR_N")

		# Add fields Range Township Section Primary_Par_FLG and SOURCE_SEQ_NBR and populate
		arcpy.AddField_management(outRecords, "RANGE", "TEXT", 2)
		arcpy.AddField_management(outRecords, "TOWNSHIP", "TEXT", 2)
		arcpy.AddField_management(outRecords, "SECTION", "LONG", 4)
		arcpy.AddField_management(outRecords, "PRIMARY_PAR_FLG", "TEXT", 1)
		arcpy.AddField_management(outRecords, "SOURCE_SEQ_NBR", "SHORT", 2)
		arcpy.AddField_management(outRecords, "SEC", "SHORT", 2)

		# Calculate Added Fields
		expression = "'Y'"
		arcpy.CalculateField_management(outRecords, "PRIMARY_PAR_FLG", expression, "PYTHON_9.3")
		expression = "1"
		arcpy.CalculateField_management(outRecords, "SOURCE_SEQ_NBR", expression, "PYTHON_9.3")
		expression = "'!RTS!.strip()[:2]'"
		cursor = arcpy.UpdateCursor(outRecords)
		for row in cursor:
			pickel = row.getValue(field)
			pickel2 = str(pickel)
			pickle2 = pickel2.strip()[:2]
			pickle3 = pickel2.strip()[2:4]
			row.setValue(field2, pickle2)
			row.setValue(field3, pickle3)
			pickle4 = pickel2.strip()[4:]
			pickle4 = int(pickle4)
			row.setValue(field4, pickle4)
			row.setValue(field5, pickle4)
			cursor.updateRow(row)

		del row
		del cursor
		arcpy.DeleteField_management(outRecords, "RTS")
		spatialJoins()

	except arcpy.ExecuteError:
	   msgs = arcpy.GetMessages(2)
	   print arcpy.AddMessage("There was a problem...script bailing")
	   arcpy.AddError(msgs)
	   print msgs


if __name__ == '__main__':
	main()
