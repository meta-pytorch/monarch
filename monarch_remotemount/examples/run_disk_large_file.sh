#!/bin/bash

( time cat /scratch/cpuhrsch/myfiledir/myfile.img > /dev/null ) 2> /tmp/total_time
cat /tmp/total_time
( time cat /scratch/cpuhrsch/myfiledir/myfile.img > /dev/null ) 2> /tmp/total_time
cat /tmp/total_time
