#!/usr/bin/perl -w
# postprocess result of an smodels/chaff run

use strict;

my $totalTime;
sub main {
	my $solution = 0;
	my @results;
	$/ = undef; # slurp mode 
	my $result = <STDIN>;
	if ($result =~ /Models\s*:\s*0/) {
		print "No results\n";
		exit 0;
	}
	for my $line (split /Answer:|Answer-set|}/, $result) { # one solution
		my @stats;
		@results = ();
		if ($line =~ /(Solving:|Duration:|tsat|c CPU time spent:|Time for first.*:)\s+([\d\.]+)/) {
			$totalTime = $2;
		}
		if ($line =~ /(.*line.*)/) { # a DLV error message
			print $line;
		}
		foreach my $component (split(/\s+/, $line)) { # one component
			if ($component =~ /(\d+)\&#(\w+)/) { # cmodels stat
				push @stats, "$2=$1";
			}
			if ($component =~ /permDistance/) {
				# print "$component\n";
			}
			next unless $component =~ /(?<!-)setting\((\d+),(\d+)\)/;
			my ($row, $value) = ($1, $2);
			$results[$row] = $value;
			# print "$component\n";
		} # each component
		if (@results) { # actual results
			$solution += 1;
			printResults($solution, @results);
			print "" . join(' ', @stats) . "\n" if @stats;
		}
	} # each line
	print "Timed out\n" if $solution == 0;
} # main

sub printCycles {
	my (@results) = @_;
	my @seen = ();
	for my $start (1 .. $#results) {
		next if defined($seen[$start]);
		print "($start";
		$seen[$start] = 1;
		my $next = $results[$start];
		while ($next != $start) {
			print " $next";
			$seen[$next] = 1;
			$next = $results[$next];
		}
		print ") ";
	} # each start
	print "\n";
} # printCycles

sub printResults {
	my ($solution, @results) = @_;
	print "Solution $solution: ";
	my %seen;
	foreach my $delta (1 .. $#results-2) {
		foreach my $index (1 .. $#results - $delta) {
			my $diff = $results[$index] - $results[$index+$delta];
			if (exists $seen{"$delta $diff"}) {
				print "at delta $delta, $diff occurs at " .
					$seen{"$delta $diff"} .
					" and at $index\n";
			}
			$seen{"$delta $diff"} = $index;
		} # each $index
	} # each delta
	foreach my $index (1 .. $#results) {
		printf " %3s", $results[$index];
	} # each row
	printf "\n\t";
	printCycles(@results);
	printf "\n";
} # printResults

main();
print "Solving time: $totalTime\n" if defined($totalTime);
