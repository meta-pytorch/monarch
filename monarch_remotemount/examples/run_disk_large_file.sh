#!/bin/bash

( time cat /tmp/myenv/myfile.img > /dev/null ) 2> /tmp/total_time
cat /tmp/total_time
( time cat /tmp/myenv/myfile.img > /dev/null ) 2> /tmp/total_time
cat /tmp/total_time
