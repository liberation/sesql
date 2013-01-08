#!/bin/sh

set -e

DIR=$1
LANG=$2

if [ -n "$DIR" ]
then
    if [ ! -d "$DIR" ]
    then
	echo "Syntax : $0 [<directory>] [<language>]"
	echo "If runned without parameters, will prompt for them."
	exit 1
    fi
fi

if [ ! -n "$DIR" ]
then
    read -p "PosgreSQL directory ? (default : /usr/share/postgresql/*/tsearch_data/)" DIR
fi

if [ ! -n "$DIR" ]
then
    DIR=/usr/share/postgresql/*/tsearch_data/
fi

if [ ! -n "$LANG" ]
then
    read -p "Language ? (default : all of them)" LANG
fi

if [ ! -n "$LANG" ]
then
    LANG=*
fi

for STOP in $DIR/$LANG.stop
do
    DNAME=`dirname $STOP`
    BNAME=`basename $STOP`
    if ! echo $BNAME | grep -q "ascii_"
    then	
	NEW=$DNAME/ascii_$BNAME
	LC_ALL=fr_FR.UTF-8 iconv -f utf-8 -t ascii//TRANSLIT $STOP > $NEW
    fi
done

    
