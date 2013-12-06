#!/usr/bin/perl -w


system "rm -f testdisk.*";
system "makedisk testdisk 1024 128 1 1024 1 100 10 .28";
system "btree_init testdisk 1024 4 4";


my $cmd1 = "btree_insert testdisk 1024 1111 7777";
my $cmd2 = "btree_insert testdisk 1024 aaaa 0000";
my $cmd3 =  "btree_insert testdisk 1024 bbbb 1111";
my $cmd4 =  "btree_insert testdisk 1024 cccc 2222";
my $cmd5 =  "btree_insert testdisk 1024 dddd 3333";
my $cmd6 =  "btree_insert testdisk 1024 eeee 4444";
my $cmd7 =  "btree_insert testdisk 1024 ffff 5555";
my $cmd8 =  "btree_insert testdisk 1024 gggg 6666";
my $cmd9 =  "btree_insert testdisk 1024 2222 8888";
my $cmd10 =  "btree_insert testdisk 1024 3333 9999";
my $cmd11 =  "btree_insert testdisk 1024 4444 aaaa";

my @commands = ($cmd1, $cmd2, $cmd3, $cmd4, $cmd5, $cmd6, $cmd7, $cmd8, $cmd9, $cmd10, $cmd11);

fisher_yates_shuffle(\@commands);

foreach my $command(@commands){
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