#!/usr/bin/env bash

update_vault_secrets() {

    source_file_path="/vault/secrets/secrets.txt"

    if [[ ! -f "$source_file_path" ]]; then
        echo "Vault secrets file not found, skipping vault injection."
        return 0
    fi

    STRING="data: map"
    if  grep -q "$STRING" "$source_file_path" ; then
        head -n 1 $source_file_path  | sed 's/data: map[][]/ /g' | sed 's/[][]/ /g'| sed 's/:/=/g' | sed 's/ /\n'/g | sed '1,1d' | sed '$d' > /vault/secrets/new_secrets.txt
        rm $source_file_path
        source_file_path="/vault/secrets/new_secrets.txt"
    else
        echo 'the string does not exist' ;
    fi

   if [[ "${source_file_path}" == "" ]]; then
        echo "File name not passed, exiting"
        exit 1
   fi

    while read -r line
    do
      export "${line?}"
    done < $source_file_path
    rm $source_file_path
}

update_vault_secrets
