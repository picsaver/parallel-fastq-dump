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
        echo -e "`printf %-16s ` [-s|--SdI <SdI> ...  get Spot Index info,default 'S1/d./I2']"
}


gzip=False

[ -z "$1" ] && show_usage && exit 0
TEMP=`getopt -o hvzs --long help,version,gzip,SdI -- "$@" 2>/dev/null`
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
                -s|--SdI)
                        SdI=$1
                        shift 2
                        ;;
                --)
                        shift
                        ;;
                *)
                        break
                        ;;
        esac
done


function fqfile_compelte_check(){
        file=$1
        open_exe=$2
        # set default
        if [[ -z $open_exe ]]; then
            open_exe="cat"
        fi
        $open_exe $file 2>/dev/null | tail -4 | tac | \
        awk 'BEGIN{name1_len=0; name2_len=0; seq_len=0; qual_len = 0; check="success"}
             NR==1{ qual_len = length($0)} 
             NR==2{if($1 !~ /^\+/){check="fail"; exit}; name2_len = length($1)}
             NR==3{
                        if($1 !~ /^[ATCGN]+$/){check="fail"; exit}; 
                        seq_len = length($1); 
                        if(seq_len != qual_len){check="fail"; exit}
                   }
             NR==4{
                        if($1 !~ /^@/){check="fail"; exit}; 
                        name1_len = length($1)
                        if(name1_len != name2_len){check="fail"; exit}
                   }
             END{print check}'
}

function get_read_index(){
    file1=$1
    open_exe=$2
    SdI=$3

    $open_exe $file1 2>/dev/null | \
    awk -v SdI="$SdI" '
	function getSpotIndex(headline, SdI){
		### SdI: get $S; d: split $S [line split as awk default] by #d [arr split] to arr; Index=arr[#I] 
		### SdI = S1/d./I2 is for "@SRR12142671.455803446 A00204:280:HHCLTDSXX:3:1463:4797:13870 length=150" and get result=455803446
		### SdI = S1/d./I2 is for "@SRR14851095.100 100 length=150" and get result=100, or use S2/d#/I1

		### set default value
		if(SdI==""){
			SdI = "S1/d./I2"
		}
       	        split(SdI, args, "/")
		gsub(/^S/, "", args[1])
		S=args[1]
		gsub(/^d/, "", args[2])
		d=args[2]
		gsub(/^I/, "", args[3])
		I=args[3]
		
                split(headline, head_arr, " ")
		split(head_arr[S], resl_arr, d)
		return resl_arr[I]
	}
    BEGIN{
        read_EOF = "true"
        headline = ""
        read_index = 0
    }
    {
        if(NR % 4 == 1){
                headline = $0
                read_EOF = "false"
        }
        if(NR % 4 == 0){
                read_EOF = "true"
        }
        
   }
   END{
        read_index = getSpotIndex(headline, SdI)
        if(read_EOF == "true"){
                print read_index
        }else{
                print read_index-1
        }
   }'
}


# echo gzip=$gzip
# echo $*

file_count=$#
file1=$1

if [[ $gzip = 'True' ]];then
    Xcat="gzip -dc"
    Xbatch="gzip -c"
else
    Xcat="cat"
    Xbatch="cat"
fi

if [[ -z $SdI ]];then
        SdI="S1/d./I2"
fi

# paste_cmd="paste -d \"\\\t\" "
# outfiles=""
# for f in $*
# do
#    fname=`basename $f`
#    outdir=`dirname $f`
#    paste_cmd=$paste_cmd" <($Xcat $f)"
#    outfiles=$outfiles" $outdir/fixed_${fname}"
# done

### step1: check fq complete
complete="true"
for fq in $*
do
        resl=`fqfile_compelte_check $fq "$Xcat"`
        if [[ $resl = "fail" ]];then
                complete="false"
                break
        fi
done

if [[ $complete = "true" ]];then
    ### use first file to get ids
    get_read_index $file1 "$Xcat" "$SdI"
else
    echo "[WARN] fastq file in `dirname $file1` is not complete, overall all files read index..." > /dev/stderr
    end_read_index=`for fq in $*
                do
                   get_read_index $fq "$Xcat" "$SdI"
                done | awk 'NR==1{min=$1} {if($1<min){min=$1}} END{print min}'`

    tmp_head_fq=`dirname $file1`/file1-head.fastq
    $Xcat $file1 | head -4 > $tmp_head_fq
    start_read_index=`get_read_index $tmp_head_fq cat "$SdI"`
    [[ -n $start_read_index ]] && rm -f $tmp_head_fq

    read_count=$(($end_read_index-$start_read_index+1))
    ### rewrite fastq
        for fq in $*
        do
                fname=`basename $fq`
                outdir=`dirname $fq`
                bakname="$outdir/bak_${fname}"
                mv $fq $bakname && \
                $Xcat $bakname 2>/dev/null | head -$(($read_count*4)) | $Xbatch > $fq
        done

        echo $end_read_index
fi




