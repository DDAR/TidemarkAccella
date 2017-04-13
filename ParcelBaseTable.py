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
propTable = r"M:\Geodatabase\Taxlots\Tables.gdb\Property"
outRecords = r"R:\Geodatabase\Taxlots\Accela.gdb\Parcel_BaseTest"
layer = "managetaxlots"

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
        arcpy.MakeFeatureLayer_management (taxSpatialRecord, layer)

        # Join Records to the properties table
        arcpy.AddJoin_management(layer, "ASSESSOR_N", propTable, "ASSESSOR_N")
        # - Test to see what fields are called so I can drop them
        #fields = arcpy.ListFields(layer)
        #for field in fields:
        #    print("{0}    -    {1} with a length of {2}").format(field.name, field.type, field.length)



        # Create the feature class in the geodatabase
        arcpy.CopyFeatures_management(layer, outRecords)
        #dropFields = ["AREA", "PERIMETER", "CNTYPARC_", "CNTYPARC_I", "ACRES", "OBJECTID_1", "ASSESSOR_N_1", "TCA", "TAX_YEAR", "USE_CODE", "LOCATED_ON", "NEW_CONST", "CU_LAND", "CU_IMPVT", "MEASURE", "SITUS_ADDR", "SITUS_ZIP", "SITUS_CITY", "CU_DATE", "CU_VALUE", "CYCLE", "NBHD", "INS_DATE", "CUR_CYCLE", "HOUSE_NO"]
        #arcpy.DeleteField_management(layer, dropFields)



    except arcpy.ExecuteError:
	msgs = arcpy.GetMessages(2)
	print arcpy.AddMessage("There was a problem...script bailing")
	arcpy.AddError(msgs)
	print msgs


if __name__ == '__main__':
    main()
