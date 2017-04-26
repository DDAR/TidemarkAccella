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
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcels_AddressTest1"
outRecords2 = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcels_AddressTest"
outRecCom = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCom"
#outRecCen = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCen"
remFields = ["AREA", "PERIMETER", "SYMBOL", "ImageURL", "BioURL", "NAME"]
remFields2 = ["STATEFP10", "COUNTYFP10", "TRACTCE10", "GEOID10", "NAMELSAD10", "MTFCC10", "FUNCSTAT10", "ALAND10", "AWATER10", "INTPTLAT10", "INTPTLON10"]
dropfieldsINSJ = ["Join_Count", "TARGET_FID"]
layer = "manageAddtaxlots"
whereClauseSEl = '"PARC" > 10000 AND "PARC" < 50000'
field = "ADDR_HN"
field2 = "HOUSE"
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
		killObject(outRecords2)
		killObject(layer)
		#---Set Evnironment Settings
		arcpy.env.workspace = r"R:\Geodatabase\Taxlots\SupportFiles"
		arcpy.env.qualifiedFieldNames = False

		# Create a layer to join records to
		arcpy.MakeFeatureLayer_management (taxSpatialRecord, layer, whereClauseSEl)

		# Join Records to the properties table
		arcpy.AddJoin_management(layer, "ASSESSOR_N", propTable, "ASSESSOR_N")

		# Create the feature class in the geodatabase and drop fields
		arcpy.CopyFeatures_management(layer, outRecords)
		dropFields = ["AREA", "PERIMETER", "CNTYPARC_", "CNTYPARC_I", "RTS", "PARC", "ACRES", "OBJECTID_1", "ASSESSOR_N_1", "TCA", "TAX_YEAR", "USE_CODE", "LOCATED_ON", "MKT_LAND", "MKT_IMPVT", "NEW_CONST", "CU_LAND",
		 "CU_IMPVT", "SIZE", "MEASURE", "CU_DATE", "CU_VALUE", "CYCLE", "NBHD", "INS_DATE",  "INS_NUM", "CUR_CYCLE", "HOUSE_NO",]
		arcpy.DeleteField_management(outRecords, dropFields)

		# Delete Identical records
		arcpy.DeleteIdentical_management(outRecords, "ASSESSOR_N")

		# Add fields
		arcpy.AddField_management(outRecords, "SOURCE_SEQ_NBR", "SHORT", 2)
		arcpy.AddField_management(outRecords, "SERV_PROV_CODE", "TEXT", 15)
		arcpy.AddField_management(outRecords, "HOUSE", "LONG")
		arcpy.AddField_management(outRecords, "UNIT", "TEXT", 10)
		arcpy.AddField_management(outRecords, "STR_SUFFIX", "TEXT", 30)
		arcpy.AddField_management(outRecords, "SITUS_STATE", "TEXT", 30)

		# Parse Address
		address_fields = "SITUS_ADDR"
		locator_style = "US Address - Single House"
		add_fields = "HouseNum;PreDir;PreType;StreetName;SufType;SufDir"
		arcpy.StandardizeAddresses_geocoding(outRecords, address_fields, locator_style, add_fields, outRecords2)

		# Calculate Added Fields
		expression = "1"
		arcpy.CalculateField_management(outRecords2, "SOURCE_SEQ_NBR", expression, "PYTHON_9.3")
		expression = "'YAKIMACO'"
		arcpy.CalculateField_management(outRecords2, "SERV_PROV_CODE", expression, "PYTHON_9.3")
		expression = "'WA'"
		arcpy.CalculateField_management(outRecords2, "SITUS_STATE", expression, "PYTHON_9.3")

		cursor = arcpy.UpdateCursor(outRecords2)
		for row in cursor:
			pickel = row.getValue(field)
##			pickel2 = int(pickel)
			row.setValue(field2, pickel)
			cursor.updateRow(row)

		del row
		del cursor

##		arcpy.DeleteField_management(outRecords, "RTS")
##		spatialJoins()
##
##		con = pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'
##		 r'SERVER=172.20.10.141;'
##		 r'DATABASE=Accela;'
##		 r'UID=Accela;'
##		 r'PWD=Pw4accela'
##		 )
##		cursort = con.cursor()
##		cursort.execute('TRUNCATE TABLE dbo.Parcel_base;')
##		con.commit()
##
##		outCursor = arcpy.SearchCursor(outRecords)
##		for row in outCursor:
##			xSource = row.getValue("SOURCE_SEQ_NBR")
##			xParcid = row.getValue("ASSESSOR_N")
##			xCensus = row.getValue("NAME10")
##			xCommish = row.getValue("COMMISH")
##			xMarketv = row.getValue("MKT_IMPVT")
##			xInsnum = row.getValue("INS_NUM")
##			xMarketLand = row.getValue("MKT_LAND")
##			xLegalLine = row.getValue("LEGAL_LINE")
##			xParc = row.getValue("PARC")
##			xParcArea = row.getValue("SIZE")
##			xTownship = row.getValue("TOWNSHIP")
##			xRangeT = row.getValue("RANGE")
##			xSection = row.getValue("SECTION")
##			xPrimary = row.getValue("PRIMARY_PAR_FLG")
##
##			#addTableRec(xSource, xParcid, xCensus, xCommish, xMarketv, xInsnum, xMarketLand, xLegalLine, xParc, xParcArea, xTownship, xRangeT, xSection, xPrimary, cursort)
##			insertStr = """INSERT INTO dbo.Parcel_base(SOURCE_SEQ_NBR, L1_PARCEL_NBR, l1_Census_tract, L1_Council_District, L1_Improved_Value, L1_Inspection_district, L1_Land_Value, L1_LEGAL_DESC, L1_Parcel, L1_Parcel_Area, GIS_ID, L1_Township, L1_Range, L1_Section, L1_Primary_Par_Flg) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14})""".format(SQLEsc2(xSource), SQLEsc2(xParcid), SQLEsc2(xCensus), SQLEsc2(xCommish), SQLEsc2(xMarketv), SQLEsc2(xInsnum), SQLEsc2(xMarketLand), SQLEsc(xLegalLine), SQLEsc2(xParc), SQLEsc2(xParcArea), SQLEsc2(xParcid), SQLEsc2(xTownship), SQLEsc2(xRangeT), SQLEsc2(xSection), SQLEsc(xPrimary))
##			#print insertStr
##			cursort.execute(insertStr)
##			con.commit()
##
##		con.close()
##		del cursort


	except arcpy.ExecuteError:
	   msgs = arcpy.GetMessages(2)
	   print arcpy.AddMessage("There was a problem...script bailing")
	   arcpy.AddError(msgs)
	   print msgs


if __name__ == '__main__':
	main()
