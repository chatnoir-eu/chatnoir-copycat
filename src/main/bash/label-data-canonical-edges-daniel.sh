#!/bin/bash -e

TO_LABEL=$(find src/main/resources/canonical-link-edges-human-judgments/judgments-by-daniel/ -type f -printf '%h\n'|grep '__to__' | sort | uniq -d)

function process_dir()
{
	echo "\nProcess ${DIR}"
	#echo "meld ${DIR}/src-doc ${DIR}/target-doc -o ${DIR}/label"
	
	echo -e "\n####################### METADATA #########################\n"
	cat ${DIR}/metadata
	echo -e "\n##########################################################\n"

	meld ${DIR}/src-doc ${DIR}/target-doc -o ${DIR}/label
}

for DIR in ${TO_LABEL[@]}
do
	if [ "4" = "$(find ${DIR}|wc -l)" ]
	then
		process_dir ${DIR}
	else
		echo "Done: ${DIR}"
	fi
done
