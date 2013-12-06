#!/usr/bin/perl -w


system "rm -f testdisk.*";
system "touch .dependencies";
system "make depend clean";
system "make";
$ENV{PATH}.=":.";
system "makedisk testdisk 1024 128 1 1024 1 100 10 .28";
system "btree_init testdisk 1024 4 4";


my @commands = ("btree_insert testdisk 1024 1 val1",
"btree_insert testdisk 1024 2 val2",
"btree_insert testdisk 1024 3 val3",
"btree_insert testdisk 1024 4 val4",
"btree_insert testdisk 1024 5 val5",
"btree_insert testdisk 1024 6 val6",
"btree_insert testdisk 1024 7 val7",
"btree_insert testdisk 1024 8 val8",
"btree_insert testdisk 1024 9 val9",
"btree_insert testdisk 1024 10 vala",
"btree_insert testdisk 1024 11 valb");

#fisher_yates_shuffle(\@commands);

foreach my $command(@commands){
	print "::::::::::::::::~ Insert Command ~::::::::::::::::\n";
	print $command;
	print "\n::::::::::::::::::::::::::::::::::::::::::::::::::\n";
	system $command;
}

sub fisher_yates_shuffle {
    my $array = shift;
    my $i;
    for ($i = @$array; --$i; ) {
        my $j = int rand ($i+1);
        next if $i == $j;
        @$array[$i,$j] = @$array[$j,$i];
    }
}