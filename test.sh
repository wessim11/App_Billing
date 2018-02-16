#!/bin/bash


for i in {1..300}
do
	sleep 0.3
	cp call /var/spool/asterisk/outgoing/call${i}
done;
