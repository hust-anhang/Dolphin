#!/bin/bash
ARGS=`getopt -o dh  -n 'build.sh' -- "$@"`
if [ $? != 0 ]; then
    echo "-h help"
    exit 1
fi
eval set -- "${ARGS}"
DEBUG_TYPE="Release"
while true
do
    case $1 in
        -h|--h)
            echo "-d:Debug"
            exit 1
            ;;
        -d)
            DEBUG_TYPE="DEBUG"
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error!"
            exit 1
            ;;
    esac
done
cd `dirname $0`
rm -rf ../build
mkdir ../build
cd ../build
if [ $DEBUG_TYPE == "DEBUG" ];then
    cmake -DCMAKE_BUILD_TYPE=Debug ..
else
    cmake ..
fi
make -j64
cp ../script/restartMemc.sh .
