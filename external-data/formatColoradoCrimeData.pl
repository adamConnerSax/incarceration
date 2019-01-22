#! /usr/bin/perl
my ($rawName) = @ARGV;
open (my $handle, $rawName)
    or die "Could not open file '$rawName'!";
# we need to do a few things
# 1. Remove top 2 lines (Report name and Date created)
my $name = <$handle>;
my $timestamp = <$handle>;

# 2. For each line we need to fix the numbers with commas, then remove quotes.  If we do it in the opposite order, the commas get confusing    

while ($line = <$handle>) {
    $line =~ s/\r\n/\n/g; # dos2unix
    $line =~ s/Jurisdiction by Geography/County/;
    $line =~ s/Incident Date/Year/;
    $line =~ s/Offense Type/CrimeAgainst/;
    $line =~ s/Number of\s//g;
    $line =~ s/Estimated Population/EstPopulation/;
    $line =~ s/\"(\d+),(\d+)\"/\"\1\2\"/g; # remove commas in quoted fields
    #    $line =~ s/(["\'])((?:\\\\\\1|.)*?)\\1/\2/g; # remove quotes
    $line =~ s/"([^"]*)"/\1/g; # remove quotes
    $line =~ s/\sCounty//g; # remove the word "County"
    $line =~ s/Crimes Against\s//g; # "remove the words "Crimes Against"    
    print $line;
}

