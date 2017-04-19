""" #####################################################
    NAME: cliplidardata.py
    Source Name:
    Version: ArcGIS 10
    Author: DD Arnett
    Usage: manage lidar geodatabases
    Required Arguments:
    Optional Arguments:
    Description: Takes the Lidar data and clips then exports the data into section files.
    Date Created: May 24, 2011
    Updated:
##################################################### """
import os
import sys
import arcpy
import pyodbc

def message(msg):
	LocalTime = time.asctime(time.localtime(time.time()))
	mmsg = msg + LocalTime; arcpy.AddMessage(mmsg); print mmsg

def killObject( object ):
	if arcpy.Exists(object):
		arcpy.Delete_management(object)

def addTableRec(source, parcid, census, commish, marketv, insnum, marketland, legalLine, parc, parcArea, township, rangeT, section, primary):
    #cxnCursor = con.cursor()
    #insertStr = """INSERT INTO dbo.Parcel_base(SOURCE_SEQ_NBR, L1_PARCEL_NBR, L1_CENSUS_TRACT, L1_COUNCIL_DISTRICT, L1_IMPROVED_VALUE, L1_INSPECTION_DISTRICT, L1_LAND_VALUE, L1_LEGAL_DESC, L1_PARCEL, L1_PARCEL_AREA, GIS_ID, L1_TOWNSHIP, L1_RANGE, L1_SECTION, L1_PRIMARY_PAR_FLG)
     #VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}')""".format(source, parcid, census, commish, marketv, insnum, marketland, legalLine, parc, parcArea, parcid, township, rangeT, section, primary)
    insertStr = """INSERT INTO dbo.Parcel_base(SOURCE_SEQ_NBR, L1_PARCEL_NBR, L1_LEGAL_DESC) VALUES ('{0}', '{1}', '{2}')""".format(xSource, xParcid, xLegalLine)
    print insertStr
    cursor.execute(insertStr)
    con.commit()

# Set Date and other variables
taxSpatialRecord = r"M:\Geodatabase\Taxlots\Taxlots.gdb\parcels"
partyTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Party"
legalTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Legal"
propTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Property"
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTest"
commishRec = r"M:\Geodatabase\boundary\districts.gdb\voting\commissioner"
outRecCom = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCom"
censusRec = r"M:\Geodatabase\census\Census 2010 Geography.gdb\Tracts"
outRecCen = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTestCen"
outFields = ["PARC", "ASSESSOR_N", "MKT_LAND", "MKT_IMPVT", "SIZE", "INS_NUM", "LEGAL_LINE", "RANGE", "TOWNSHIP", "SECTION", "PRIMARY_PAR_FLG", "SOURCE_SEQ_NBR"]
remFields = ["AREA", "PERIMETER", "SYMBOL", "ImageURL", "BioURL", "NAME"]
fieldCom = "COMMISH"
dropfieldsINSJ = ["Join_Count", "TARGET_FID", "RTS"]
remFields2 = ["STATEFP10", "COUNTYFP10", "TRACTCE10", "GEOID10", "NAMELSAD10", "MTFCC10", "FUNCSTAT10", "ALAND10", "AWATER10", "INTPTLAT10", "INTPTLON10"]
fieldsRR = ["SOURCE_SEQ_NBR", "L1_PARCEL_NBR", "L1_PARCEL_STATUS", "L1_BLOCK", "L1_BOOK", "L1_CENSUS_TRACT", "L1_COUNCIL_DISTRICT", "L1_EXEMPT_VALUE", \
 "L1_GIS_SEQ_NBR", "L1_IMPROVED_VALUE", "L1_INSPECTION_DISTRICT", "L1_LAND_VALUE", "L1_LEGAL_DESC", "L1_LOT", "L1_MAP_NBR", "L1_MAP_REF", "L1_PAGE", "L1_PARCEL", "L1_PARCEL_AREA", \
 "L1_PLAN_AREA", "L1_SUPERVISOR_DISTRICT", "L1_TRACT", "GIS_ID", "L1_SUBDIVISION", "L1_TOWNSHIP", "L1_RANGE", "L1_SECTION", "L1_PRIMARY_PAR_FLG", "EXT_UID"]
fieldsTT = ["SOURCE_SEQ_NBR", "ASSESSOR_N", "NAME10", "COMISH", "MKT_IMPVT", "INS_NUM", "MKT_LAND", "LEGAL_LINE", "PARC", "SIZE", "ASSESSOR_N", "TOWNSHIP", "RANGE", "SECTION", "PRIMARY_PAR_FLG"]

try:
    con = pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'
     r'SERVER=172.20.10.141;'
     r'DATABASE=Accela;'
     r'UID=Accela;'
     r'PWD=Pw4accela'
     )
    cursor = con.cursor()
    cursor.execute('TRUNCATE TABLE dbo.Parcel_base;')
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
        xxLegalLine = row.getValue("LEGAL_LINE")
        if xxLegalLine is None:
            xLegalLine = ''
        else:
            xLegalLine =  xxLegalLine.replace("'", "''")
        xParc = row.getValue("PARC")
        xParcArea = row.getValue("SIZE")
        xTownship = row.getValue("TOWNSHIP")
        xRangeT = row.getValue("RANGE")
        xSection = row.getValue("SECTION")
        xPrimary = row.getValue("PRIMARY_PAR_FLG")

        addTableRec(xSource, xParcid, xCensus, xCommish, xMarketv, xInsnum, xMarketLand, xLegalLine, xParc, xParcArea, xTownship, xRangeT, xSection, xPrimary)



    con.close()
    del cursor

except IOError as e:
    print 'Exception error is: %s' % e









