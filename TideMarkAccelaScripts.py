#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      donnaa
#
# Created:     05/05/2017
# Copyright:   (c) donnaa 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import subprocess

def main():
    subprocess.call("ParcelAddressTable.py", shell = True)
    subprocess.call("ParcelAttributes.py", shell=True)

if __name__ == '__main__':
    main()
