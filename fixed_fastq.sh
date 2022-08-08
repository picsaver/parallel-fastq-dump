#!/usr/bin/env bash

show_version()
{
        echo -e "\033[31mVersion: 1.0\033[0m"
        echo "Author: Deng Longhui"
        echo "Updated date: 2022-08\n"
}

show_usage()
{
        echo -e "`printf %-16s "Usage: $0"`"
        echo -e "`printf %-16s ` [-h|--help]"
        echo -e "`printf %-16s ` [-v|--version]"
        echo -e "`printf %-16s ` [-z|--gzip ... Whether input file is gziped,required]"
}


gzip=False

[ -z "$1" ] && show_usage && exit 0
TEMP=`getopt -o hvz --long help,version,gzip -- "$@" 2>/dev/null`
eval set -- "$TEMP"
while :
do
        case "$1" in
                -h|--help)
                        show_usage; exit 0
                        ;;
                -v|--version)
                        show_version; exit 0
                        ;;
                -z|--gzip)
                        gzip=True
                        shift 1
                        ;;
                --)
                        shift
                        ;;
                *)
                        break
                        ;;
        esac
done

# echo gzip=$gzip
# echo $*

file_count=$#


if [[ $gzip = 'True' ]];then
    Xcat="gzip -dc"
else
    Xcat="cat"
fi

paste_cmd="paste -d \"\\\t\" "
outfiles=""
for f in $*
do
   fname=`basename $f`
   outdir=`dirname $f`
   paste_cmd=$paste_cmd" <($Xcat $f)"
   outfiles=$outfiles" $outdir/fixed_${fname}"
done


bash -c "$paste_cmd" | \
    awk -F "\t" -v filecount=$file_count -v gzip=$gzip -v output="$outfiles" '
    function InitArr(arr){
            for(i=1;i<=filecount;i++){
                    arr[i] = ""
            }
    }
    function WritedArr(arr, files, pipetools){
            sucess = 0
            for(i=1; i<=filecount; i++){
                    if(arr[i]  == "") continue
                    print arr[i] | pipetools" >> "files[i]
                    sucess = 1
            }
            return sucess
    }
    function AddSwap(arr, swap_arr, sep){
           for(i=1; i<=filecount; i++){
                if(swap_arr[i]  == "") continue
                if(arr[i] == ""){
                        use_sep=""  
                }else{
                        use_sep=sep
                }
                arr[i] = arr[i]""use_sep""swap_arr[i]
            }
    }
    BEGIN{
            farr_len = split(output, farr, " ");
            if(farr_len != filecount){ exit; }
            if(gzip == "True"){
                    pipetools="gzip -c"
            }else{
                    pipetools="cat"
            }
           for(i=1;i<=filecount;i++){
                read_arr[i] = ""
            }
           for(i=1;i<=filecount;i++){
                batch_arr[i] = ""
            }
           batch_count = 100
           read_count = 0

    }
    
    {
        # store info handle
        if(NR % 4 == 1){
                read_count = read_count + 1
        }
        if(NR % 4 == 1 && batch_count <= read_count){
                WritedArr(batch_arr, farr, pipetools)
                InitArr(batch_arr)
                InitArr(read_arr)
                read_count = 0
        }
        if(NR % 4 == 1 && batch_count > read_count){
                AddSwap(batch_arr, read_arr, "\n")
                InitArr(read_arr)  
        }
        # current line/read handle
        if (NF != filecount){
                ### terminate program at any line
                # directly to END part
                exit
        }

        for(i=1;i<=NF;i++){
                line_arr[i] = $i
        }
        AddSwap(read_arr, line_arr, "\n")
   }
   
   END{
        WritedArr(batch_arr, farr, pipetools)
   }'
