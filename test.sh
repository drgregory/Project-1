if [ $# -ne 2 -a $# -ne 3 ]
then
    echo "Please give me a denom value and then a file name..."
    echo "Include optional directory last for custom output directory..."
    exit 1
fi
progToRun=SmallWorld
echo "Getting rid of directories..."
if [ $# -eq 3 ]
then
rm -rf $3
else
rm -rf test_out
fi
for i in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
do
rm -rf bfs-$i-out
done
echo "Compiling Java code..."
make
echo "About to run with your test file..."
dirOut=test_out
if [ $# -eq 3 ]
then
    dirOut=$3
fi
if [ $2 = "-r" ]
then
    echo "You are running the ring test..."
    if [ $# -eq 2 ]
    then
	echo "Automatic output directory is test_out"
	hadoop jar sw.jar $progToRun ~cs61c/p1data/ring4.seq $dirOut $1
    else
	echo "Using custom directory $3..."
	hadoop jar sw.jar $progToRun ~cs61c/p1data/ring4.seq $dirOut $1
    fi
fi
if [ $# -eq 3 ]
then
    dirOut=$3
fi
if [ $2 = "-h" ]
then
    echo "You are running the high energy physics test..."
    if [ $# -eq 2 ]
    then
	echo "Automatic output directory is test_out..."
	if [ $1 -lt 10000 ]
	then
	    echo "Forcing denom value to be 10000 minimum to not overrun disk quota..."
	    hadoop jar sw.jar $progToRun ~cs61c/p1data/cit-HepPh.sequ $dirOut 10000
	else
	    echo "Using your denom value of $1..."
	    hadoop jar sw.jar $progToRun ~cs61c/p1data/cit-HepPh.sequ $dirOut $1
	fi
    else
	echo "Using custom output directory $3..."
	if [ $1 -lt 10000 ]
	then
	    echo "Forcing denom value to be 10000 minimum to not overrun disk quota..."
	    hadoop jar sw.jar $progToRun ~cs61c/p1data/cit-HepPh.sequ $dirOut 10000
	else
	    echo "Using your denom value of $1..."
	    hadoop jar sw.jar $progToRun ~cs61c/p1data/cit-HepPh.sequ $dirOut $1
	fi
    fi
fi
while [ true ]
do
    echo "Do you want to see your output? Type yes (y) or no (n)"
    read answer
    if [ $answer = "yes" -o $answer = "y" ]
    then
	echo "Going to show your output..."
	if [ $# -eq 3 ]
	then
	    cat $3/part-r-00000
	else
	    cat test_out/part-r-00000
	fi
	echo ""
	echo "Goodbye..."
	exit 0
    else
	if [ $answer = "no" -o $answer = "n" ]
	then
	    echo "Goodbye..."
	    exit 0
	fi
    fi
done
exit 0
