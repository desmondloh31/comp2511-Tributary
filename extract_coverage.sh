#!/bin/bash

cd build/reports/jacoco/test/html/

row=`cat index.html | grep -o '<td>Total<\/td>.*<td class="ctr2">[0-9][0-9]%<\/td>'`

percent=`echo $row | grep -o -m2 '[0-9][0-9]' | tail -n1`

echo "${percent}%"

# Uncomment this if you would like to have CI enforcing coverage scores
# (not mandatory for  this assignment)
#
# if [ $percent -lt 80 ]
# then
#   >&2 echo 'Test coverage is less then 80%'
#   exit 1
# fi
