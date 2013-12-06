#!/usr/bin/perl -w


system "rm -f testdisk.*";
system "touch .dependencies";
system "make depend clean";
system "make";
$ENV{PATH}.=":.";
system "makedisk testdisk 1024 128 1 1024 1 100 10 .28";
system "btree_init testdisk 1024 4 4";


my @commands = ("btree_insert testdisk 1024 1111 aaaa",
"btree_insert testdisk 1024 2222 aaaa",
"btree_insert testdisk 1024 3333 aaaa",
"btree_insert testdisk 1024 4444 aaaa",
"btree_insert testdisk 1024 5555 aaaa",
"btree_insert testdisk 1024 6666 aaaa",
"btree_insert testdisk 1024 7777 aaaa",
"btree_insert testdisk 1024 8888 aaaa",
"btree_insert testdisk 1024 9999 aaaa",
"btree_insert testdisk 1024 9998 aaaa",
"btree_insert testdisk 1024 9997 aaaa");

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