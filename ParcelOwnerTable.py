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
reload(sys)
sys.setdefaultencoding('utf8')


# Variables ---------------------------
taxSpatialRecord = r"M:\Geodatabase\Taxlots\Taxlots.gdb\parcels"
partyTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Party"
legalTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Legal"
propTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Property"
commishRec = r"M:\Geodatabase\boundary\districts.gdb\voting\commissioner"
censusRec = r"M:\Geodatabase\census\Census 2010 Geography.gdb\Tracts"
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_OwnerTest"
outRecCom = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCom"
#outRecCen = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCen"
remFields = ["AREA", "PERIMETER", "SYMBOL", "ImageURL", "BioURL", "NAME"]
remFields2 = ["STATEFP10", "COUNTYFP10", "TRACTCE10", "GEOID10", "NAMELSAD10", "MTFCC10", "FUNCSTAT10", "ALAND10", "AWATER10", "INTPTLAT10", "INTPTLON10"]
dropfieldsINSJ = ["Join_Count", "TARGET_FID"]
layer = "manageOwner"
whereClauseSEl = '"PARC" > 10000 AND "PARC" < 50000'
field = "ORG_NAME"
field2 = "FIRST_NAME"
field3 = "LAST_NAME"


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
        return "NULL"
    else:
        return s

def isNotEmpty(s):
    return bool(s and s.strip())

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
		arcpy.AddJoin_management(layer, "ASSESSOR_N", partyTable, "ASSESSOR_N")

		# Create the feature class in the geodatabase and drop fields
		arcpy.CopyFeatures_management(layer, outRecords)
		dropFields = ["AREA", "PERIMETER", "CNTYPARC_", "CNTYPARC_I", "RTS", "PARC", "ACRES", "ROLE", "STATUS", "ROLE_PERCE", "EFF_FROM_D", "OBJECTID_1", "ASSESSOR_N_1" ]
		arcpy.DeleteField_management(outRecords, dropFields)

		# Delete Identical records
		arcpy.DeleteIdentical_management(outRecords, "ASSESSOR_N")

		# Add fieldSource_seq_nbr
		arcpy.AddField_management(outRecords, "SOURCE_SEQ_NBR", "SHORT", 4)

		# Calculate Added Fields and update Organization Name
		expression = "1"
		arcpy.CalculateField_management(outRecords, "SOURCE_SEQ_NBR", expression, "PYTHON_9.3")
		cursor = arcpy.UpdateCursor(outRecords)
		for row in cursor:
			pickel = row.getValue(field)
			if isNotEmpty(pickel):
				pass
			else:
				pickle2 = row.getValue(field2)
				pickle3 = row.getValue(field3)
				imputString = str(pickle2) + " " + str(pickle3)
				row.setValue(field, imputString)
				cursor.updateRow(row)

		del row
		del cursor

        # Import table into SQL server database
		con = pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'
		 r'SERVER=172.20.10.141;'
		 r'DATABASE=Accela;'
		 r'UID=Accela;'
		 r'PWD=Pw4accela'
		 )
		cursort = con.cursor()
		cursort.execute('TRUNCATE TABLE dbo.Parcel_Owner;')
		con.commit()

		outCursor = arcpy.SearchCursor(outRecords)
		for row in outCursor:
			xSource = row.getValue("SOURCE_SEQ_NBR")
			xParcid = row.getValue("ASSESSOR_N")
			xorgName = row.getValue("ORG_NAME")
			xfirstName = row.getValue("FIRST_NAME")
			xmiddleName = row.getValue("MIDDLE_NAM")
			xlastName = row.getValue("LAST_NAME")
			xymailingAdd = row.getValue("MAILING_AD")
			if xymailingAdd is None:
				xmailingAdd = None
			else:
				xmailingAdd = xymailingAdd[0:39]
			xcity = row.getValue("MAILING_CI")
			xstate = row.getValue("STATE")
			xzip = row.getValue("ZIP_CODE")

			# Build the insert string used to populate SQL database
			#insertStr = """INSERT INTO dbo.Parcel_Owner(SOURCE_SEQ_NBR, L1_PARCEL_NBR, L1_OWNER_FULL_NAME, L1_OWNER_FNAME, L1_OWNER_MNAME, L1_OWNER_LNAME, L1_ADDRESS1, L1_CITY, L1_STATE, L1_ZIP) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})""".format(SQLEsc2(xSource), SQLEsc2(xParcid), SQLEsc(xorgName), SQLEsc2(xfirstName), SQLEsc2(xmiddleName), SQLEsc2(xlastName), SQLEsc(xmailingAdd), SQLEsc2(xcity), SQLEsc2(xstate), SQLEsc2(xzip))
			insertStr = """INSERT INTO dbo.Parcel_Owner(SOURCE_SEQ_NBR, L1_PARCEL_NBR, L1_OWNER_FULL_NAME, L1_OWNER_FNAME, L1_OWNER_MNAME, L1_OWNER_LNAME, L1_ADDRESS1) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6})""".format(SQLEsc2(xSource), SQLEsc2(xParcid), SQLEsc(xorgName), SQLEsc(xfirstName), SQLEsc(xmiddleName), SQLEsc(xlastName), SQLEsc(xmailingAdd))
			# Populate the SQL database
			#arcpy.AddMessage(insertStr)
			cursort.execute(insertStr)
			con.commit()

		con.close()
		del cursort

	except arcpy.ExecuteError:
	   msgs = arcpy.GetMessages(2)
	   print arcpy.AddMessage("There was a problem...script bailing")
	   arcpy.AddError(msgs)
	   print msgs


if __name__ == '__main__':
	main()
