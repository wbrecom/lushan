#!/bin/sh

# resolve links - $0 may be a softlink
THIS="$0"
while [ -h "$THIS" ]; do
    ls=`ls -ld "$THIS"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '.*/.*' > /dev/null; then
        THIS="$link"
    else
        THIS=`dirname "$THIS"`/"$link"
    fi
done

THIS_DIR=`dirname "$THIS"`
HOME=`cd "$THIS_DIR" ; pwd`

. $HOME/../conf/hyper_dict.conf

if [ -e $HDB_PATH/hyper_dict.work ];then
    exit 0
fi

touch $HDB_PATH/hyper_dict.work

if [ -e $HDB_PATH/hyper_dict.stop ];then
    rm $HDB_PATH/hyper_dict.stop
fi

LINK_PATH=$HDB_PATH/link

while [ 1 -eq 1 ];
do
	if [ -e $HDB_PATH/hyper_dict.stop ];then
		rm $HDB_PATH/hyper_dict.stop
		echo -n -e "stop\r\n" | nc 127.0.0.1 $PORT >/dev/null
		break
	fi
	echo -n -e "info\r\n" | nc 127.0.0.1 $PORT >/dev/null
	if [ $? != 0 ];
	then
		if [ -e $HDB_PATH/hyper_dict.init ];then
			rm $HDB_PATH/hyper_dict.init
		fi

		i=0
		while [ $i -lt $HDICT_NUM ];
		do
			if [ -L $LINK_PATH/$i ];then
				rm -f $LINK_PATH/$i
			fi

			choiceid=$(ls -t $HDB_PATH/$i/hdict*/choice.flg 2>/dev/null |head -1| awk -F/ '{print $(NF-1)}')
			if [ "$choiceid"x != x ];then
				echo $choiceid
				ln -s $HDB_PATH/$i/$choiceid $LINK_PATH/$i
				echo -n -e "open $LINK_PATH/$i $i\r\n" >> $HDB_PATH/hyper_dict.init
			fi

			i=$(expr $i + 1 )
			
		done
		echo -n -e "end\r\n" >> $HDB_PATH/hyper_dict.init

		$HOME/hyper_dict -t $NUM_THREADS -T $TIMEOUT -c $MAXCONNS -p $PORT -d -v -i $HDB_PATH/hyper_dict.init > $HOME/../logs/hyper_dict.log 2>&1
		i=0
		while [ 1 -eq 1 ];do
			sleep 1
			echo -n -e "info\r\n" | nc 127.0.0.1 $PORT >/dev/null
			if [ $? -eq 0 ];then
				date +"restart hyper_dict  %F %T. ok  "
				if [ -e $HDB_PATH/hyper_dict.init ];then
					rm $HDB_PATH/hyper_dict.init
				fi
				break
			fi
			i=$(expr $i + 1)
			if [ $i -ge 20 ];then
				date +"restart hyper_dict  %F %T. failed  "
				break
			fi
			echo "FAIL"
		done

		if [ $i -ge 20 ];then
			continue
		fi

	fi

	i=0
	while [ $i -lt $HDICT_NUM ];
	do
		dataids=$(ls -t $UPLOAD_PATH/$i/hdict*/done.flg 2>/dev/null | awk -F/ '{print $(NF-1)}' |awk 'BEGIN{ids="";}{if(length(ids)==0){ids=$1;}else {ids=ids" "$1} }END{print ids}')
		if [ "$dataids"x != x ];then
			first=1
			for dataid in $dataids;
			do
				if [ $first -eq 1 ];then
					mv $UPLOAD_PATH/$i/$dataid $HDB_PATH/$i/
					first=0
				else
					rm -fr $UPLOAD_PATH/$i/$dataid
				fi
			done

			dataids=$(ls -t $HDB_PATH/$i/hdict*/done.flg 2>/dev/null | awk -F/ '{print $(NF-1)}' |awk 'BEGIN{ids="";}{if(length(ids)==0){ids=$1;}else {ids=ids" "$1} }END{print ids}')
			if [ "$dataids"x != x ];then
				first=1
				for dataid in $dataids;
				do
					if [ $first -eq 1 ];then
						touch $HDB_PATH/$i/$dataid/choice.flg
						if [ -L $LINK_PATH/$i ];then
							rm $LINK_PATH/$i
						fi
						ln -s $HDB_PATH/$i/$dataid $LINK_PATH/$i

						k=0
						while [ $k -lt 5 ];
						do
							status=$(echo -n -e "open $LINK_PATH/$i $i\r\n" | nc 127.0.0.1 $PORT)
							expected=$(echo -e "OPENED\r\n")
							if [ "$status"x != "$expected"x ];
							then
								sleep 3
							else
								break
							fi
							k=$(expr $k + 1 )
						done

						first=0
					else
						if [ -e $HDB_PATH/$i/$dataid/choice.flg ]; then
							rm $HDB_PATH/$i/$dataid/choice.flg
						fi
						rm -fr $HDB_PATH/$i/$dataid
					fi
				done
			fi
		fi
		i=$(expr $i + 1 )

	done

	sleep 3
done

rm $HDB_PATH/hyper_dict.work

exit 0
