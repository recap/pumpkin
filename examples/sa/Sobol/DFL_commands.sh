#!/bin/bash

RUNFILE="./DFL_commands.R"


#if [ -z $X509_USER_PROXY ]; then
#	export R_LIBS=$PWD
#else
#	export R_LIBS=$PWD
#	module load r/2.9.2
#fi

if [ -f ./$RUNFILE ]; then
	R CMD BATCH $RUNFILE > /tmp/test_file
else
	echo no file $RUNFILE > /tmp/test_file
fi
