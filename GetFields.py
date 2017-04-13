#-------------------------------------------------------------------------------
# Name:        GetFields
# Purpose:      Creates a textfile with all the fields and their attributes
#
# Author:      donnaa
#
# Created:     13/04/2017
# Copyright:   (c) donnaa 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import arcpy

def killObject( object ):
    if arcpy.Exists(object):
        arcpy.Delete_management(object)

Record = r"M:\Geodatabase\Taxlots\Accela.gdb\Parcel_Base"
FileRec = r"D:\Data\DDA\python\Tidemark\TidemarkAccella\ParcelBaseFields.txt"
fields = arcpy.ListFields(Record)

killObject(FileRec)
f = open(FileRec, 'w')

for field in fields:
    t = ("{0}    -    {1} with a length of {2}").format(field.name, field.type, field.length)
    t = t + '\n'
    f.write(t)

        # - Test to see what fields are called so I can drop them
        #fields = arcpy.ListFields(layer)
        #for field in fields:
        #    print("{0}    -    {1} with a length of {2}").format(field.name, field.type, field.length)

f.close()