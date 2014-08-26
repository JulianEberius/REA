#!/bin/sh
pushd target/
FILE=`ls rea-*-dependencies.jar`
cp $FILE ../
popd

zip -d $FILE "rea/*"
mv $FILE rea-deps.jar
