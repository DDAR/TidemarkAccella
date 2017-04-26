#-------------------------------------------------------------------------------
# Name:		ParcelAddressTable.py
# Purpose:	 To create a table from parcels and parcel tables to be inputed
#			  into the Tidemark-Accella system
#
# Author:	  donnaa
#
# Created:	 26/04/2017
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
reload(sys)
sys.setdefaultencoding('utf8')


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
remFields2 = ["STATEFP10", "COUNTYFP10", "TRACTCE10", "GEOID10", "NAMELSAD10", "MTFCC10", "FUNCSTAT10", "ALAND10", "AWATER10", "INTPTLAT10", "INTPTLON10"]
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

def SQLEsc(s):
    if s == None:
        return "NULL"
    else:
        return "'"+string.replace(s, "'", "''")+"'"

def SQLEsc2(s):
    if s == None:
        return "NULL"
    else:
        return s

##def addTableRec(source, parcid, census, commish, marketv, insnum, marketland, legalLine, parc, parcArea, township, rangeT, section, primary, cursort):
##    try:
##        #cxnCursor = con.cursor()
##        #insertStr = """INSERT INTO dbo.Parcel_base(SOURCE_SEQ_NBR, L1_PARCEL_NBR, L1_CENSUS_TRACT, L1_COUNCIL_DISTRICT, L1_IMPROVED_VALUE, L1_INSPECTION_DISTRICT, L1_LAND_VALUE, L1_LEGAL_DESC, L1_PARCEL, L1_PARCEL_AREA, GIS_ID, L1_TOWNSHIP, L1_RANGE, L1_SECTION, L1_PRIMARY_PAR_FLG)
##         #VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}')""".format(source, parcid, census, commish, marketv, insnum, marketland, legalLine, parc, parcArea, parcid, township, rangeT, section, primary)
##        insertStr = """INSERT INTO dbo.Parcel_base(SOURCE_SEQ_NBR, L1_PARCEL_NBR, l1_Census_tract, L1_Council_District, L1_Improved_Value, L1_Inspection_district, L1_Land_Value, L1_LEGAL_DESC, L1_Parcel, L1_Parcel_Area, GIS_ID, L1_Township, L1_Range, L1_Section, L1_Primary_Par_Flg) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14})""".format(SQLEsc2(source), SQLEsc2(parcid), SQLEsc2(census), SQLEsc2(commish), SQLEsc2(marketv), SQLEsc2(insnum), SQLEsc2(marketland), SQLEsc(legalLine), SQLEsc2(parc), SQLEsc2(parcArea), SQLEsc2(parcid), SQLEsc2(township), SQLEsc2(rangeT), SQLEsc2(section), SQLEsc(primary))
##        #print insertStr
##        cursort.execute(insertStr)
##        con.commit()
##    except IOError as e:
##        print "Error in AddTable Rec"
##        print 'Exception error is: %s' % e
##        print insertStr


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

		con = pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'
		 r'SERVER=172.20.10.141;'
		 r'DATABASE=Accela;'
		 r'UID=Accela;'
		 r'PWD=Pw4accela'
		 )
		cursort = con.cursor()
		cursort.execute('TRUNCATE TABLE dbo.Parcel_base;')
		con.commit()

		outCursor = arcpy.SearchCursor(outRecords)
		for row in outCursor:
			xSource = row.getValue("SOURCE_SEQ_NBR")
			xParcid = row.getValue("ASSESSOR_N")
			xCensus = row.getValue("NAME10")
			xCommish = row.getValue("COMMISH")
			xMarketv = row.getValue("MKT_IMPVT")
			xInsnum = row.getValue("INS_NUM")
			xMarketLand = row.getValue("MKT_LAND")
			xLegalLine = row.getValue("LEGAL_LINE")
			xParc = row.getValue("PARC")
			xParcArea = row.getValue("SIZE")
			xTownship = row.getValue("TOWNSHIP")
			xRangeT = row.getValue("RANGE")
			xSection = row.getValue("SECTION")
			xPrimary = row.getValue("PRIMARY_PAR_FLG")

			#addTableRec(xSource, xParcid, xCensus, xCommish, xMarketv, xInsnum, xMarketLand, xLegalLine, xParc, xParcArea, xTownship, xRangeT, xSection, xPrimary, cursort)
			insertStr = """INSERT INTO dbo.Parcel_base(SOURCE_SEQ_NBR, L1_PARCEL_NBR, l1_Census_tract, L1_Council_District, L1_Improved_Value, L1_Inspection_district, L1_Land_Value, L1_LEGAL_DESC, L1_Parcel, L1_Parcel_Area, GIS_ID, L1_Township, L1_Range, L1_Section, L1_Primary_Par_Flg) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14})""".format(SQLEsc2(xSource), SQLEsc2(xParcid), SQLEsc2(xCensus), SQLEsc2(xCommish), SQLEsc2(xMarketv), SQLEsc2(xInsnum), SQLEsc2(xMarketLand), SQLEsc(xLegalLine), SQLEsc2(xParc), SQLEsc2(xParcArea), SQLEsc2(xParcid), SQLEsc2(xTownship), SQLEsc2(xRangeT), SQLEsc2(xSection), SQLEsc(xPrimary))
			#print insertStr
			cursort.execute(insertStr)
			con.commit()

		con.close()
		del cursort

##        # Access the database and remove the current data
##		con = pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};' r'SERVER=172.20.10.141;' r'DATABASE=Accela;' r'UID=Accela;' r'PWD=Pw4accela')
##		cursor = con.cursor()
##		cursor.execute('TRUNCATE TABLE dbo.Parcel_base;')
##		con.commit()
##		con.close()
##
##        # Insert new table into table
##		del cursor



	except arcpy.ExecuteError:
	   msgs = arcpy.GetMessages(2)
	   print arcpy.AddMessage("There was a problem...script bailing")
	   arcpy.AddError(msgs)
	   print msgs


if __name__ == '__main__':
	main()
