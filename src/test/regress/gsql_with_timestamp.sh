append_timestamp ()
{
    trans_name=$1
    while IFS= read -r line; do
        printf '[%s %s] [%s] %s\n' "$(date +'%y_%m_%d')" "$(date +'%H_%M_%S')" "$trans_name" "$line";
    done
}

#gsql ${@:2} 2>&1 | append_timestamp "$1"

