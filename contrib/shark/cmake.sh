#!/bin/bash
rm -f shark--2.1.sql
touch shark--2.1.sql
cat shark--1.0.sql >> shark--2.1.sql
cat upgrade_script/shark--1.0--2.0.sql upgrade_script/shark--2.0--2.1.sql >> shark--2.1.sql
