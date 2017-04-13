#-------------------------------------------------------------------------------
# Name:        ParcelBaseTable.py
# Purpose:     To create a table from parcels and parcel tables to be inputed
#              into the Tidemark-Accella system
#
# Author:      donnaa
#
# Created:     13/04/2017
# Copyright:   (c) donnaa 2017
# Licence:     <your licence>
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
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTest"
layer = "managetaxlots"
whereClauseSEl = '"PARC" > 10000 AND "PARC" < 50000'

# Methods   ---------------------------

def killObject( object ):
    if arcpy.Exists(object):
        arcpy.Delete_management(object)


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
        # Calculate Added Fields
        expression = "'Y'"
        arcpy.CalculateField_management(outRecords, "PRIMARY_PAR_FLG", expression, "PYTHON_9.3")
        expression = "1"
        arcpy.CalculateField_management(outRecords, "SOURCE_SEQ_NBR", expression, "PYTHON_9.3")
        expression = "'!RTS!.strip()[:2]'"




    except arcpy.ExecuteError:
	msgs = arcpy.GetMessages(2)
	print arcpy.AddMessage("There was a problem...script bailing")
	arcpy.AddError(msgs)
	print msgs


if __name__ == '__main__':
    main()
