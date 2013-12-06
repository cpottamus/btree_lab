#!/usr/bin/perl -w


system "rm -f testdisk.*";
system "touch .dependencies";
system "make depend clean";
system "make";
$ENV{PATH}.=":.";
system "makedisk testdisk 1024 128 1 1024 1 100 10 .28";
system "btree_init testdisk 1024 4 4";


my @commands = ("btree_insert testdisk 1024 1111 7777",
"btree_insert testdisk 1024 aaaa 0000",
"btree_insert testdisk 1024 bbbb 1111",
"btree_insert testdisk 1024 cccc 2222",
"btree_insert testdisk 1024 dddd 3333",
"btree_insert testdisk 1024 eeee 4444",
"btree_insert testdisk 1024 ffff 5555",
"btree_insert testdisk 1024 gggg 6666",
"btree_insert testdisk 1024 2222 8888",
"btree_insert testdisk 1024 3333 9999",
"btree_insert testdisk 1024 4444 aaaa");

fisher_yates_shuffle(\@commands);

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