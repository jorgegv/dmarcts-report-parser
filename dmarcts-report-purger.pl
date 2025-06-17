#!/usr/bin/perl

################################################################################
# dmarcts-report-purger - A Perl based tool to periodically purge DMARC
# reports older than the retention period
#
# Copyright (C) 2025 Jorge Gonzalez, based on work by TechSneeze.com and
# John Bieling
#
# Available at:
# https://github.com/jorgegv/dmarcts-report-parser
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of  MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

use Modern::Perl;
use DBI;
use File::Basename;
use Getopt::Std;
use POSIX qw(strftime);
use DateTime;

my $scriptname = basename( $0 );

our ( $dbtype, $dbname, $dbuser, $dbpass, $dbhost, $dbport, $db_tx_support, $debug );

#$debug = 1;

sub show_usage {
    print <<EOF_USAGE
usage: $scriptname [-t <target_date>] [-b <days_before_today>]
    <target_date> in format 'YYYY-MM-DD'
    - Purge all reports before the target date
EOF_USAGE
        ;
}

# locate conf file or die
my $conf_file = 'dmarcts-report.conf';
if ( -e $conf_file ) {

    #$conf_file = "./$conf_file";
} elsif ( -e ( File::Basename::dirname( $0 ) . "/$conf_file" ) ) {
    $conf_file = ( File::Basename::dirname( $0 ) . "/$conf_file" );
} else {
    show_usage();
    die "$scriptname: Could not read config file '$conf_file' from current working directory or path ("
        . File::Basename::dirname( $0 ) . ')';
}

# load conf file with error handling
if ( substr( $conf_file, 0, 1 ) ne '/' and substr( $conf_file, 0, 1 ) ne '.' ) {
    $conf_file = "./$conf_file";
}
my $conf_return = do $conf_file;
die "$scriptname: couldn't parse $conf_file: $@" if $@;
die "$scriptname: couldn't do $conf_file: $!" unless defined $conf_return;
print "Configuration loaded\n" if $debug;

# parse cli options
# -t <date>: aggregate data for a given date in format YYYY-MM-DD
# -b <n>: aggregate data for today -n days (n=1 -> yesterday)
our ( $opt_t, $opt_b, $opt_h );
getopts( "t:b:h" );
defined( $opt_h ) and do {
    show_usage();
    exit;
};

my $target_date = $opt_t || undef;
my $days_before = $opt_b || undef;
defined( $opt_t ) or defined( $opt_b ) or do {
    show_usage();
    exit;
};

if ( defined( $target_date ) ) {
    ( $target_date =~ m/^(\d{4})\-(\d{2})\-(\d{2})$/ )
        or die "$scriptname: <target_date> for -t must be in format YYYY-MM-DD\n";
} else {
    ( defined( $days_before ) and ( $days_before =~ /^(\d+)$/ ) )
        or die "$scriptname: <n> for -b must be an integer\n";
    my $tmp_datetime = DateTime->now()->subtract( days => $1 );
    $target_date = $tmp_datetime->strftime( '%Y-%m-%d' );
}
printf( "Target date: %s\n", $target_date ) if $debug;

# Setup connection to database server.
$db_tx_support = 1;
our %dbx;
my $dbx_file   = File::Basename::dirname( $0 ) . "/dbx_$dbtype.pl";
my $dbx_return = do $dbx_file;
die "$scriptname: couldn't load DB definition for type $dbtype: $@" if $@;
die "$scriptname: couldn't load DB definition for type $dbtype: $!" unless defined $dbx_return;

my $dbh = DBI->connect( "DBI:$dbtype:database=$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass )
    or die "$scriptname: Cannot connect to database\n";
if ( $db_tx_support ) {
    $dbh->{AutoCommit} = 0;
}
print "Database connection established\n" if $debug;

# prepare queries
# They must be executed in this order, otherwise the rptrecord one will do nothing!
my $delete_records = "DELETE FROM rptrecord WHERE serial IN ( SELECT serial FROM report WHERE mindate < ? )";
my $delete_reports = "DELETE FROM report WHERE mindate < ?";

# purge data
foreach my $query ( $delete_records, $delete_reports ) {
    my $qh = $dbh->prepare( $query )
        or die "Can't prepare statement: " . $dbh->errstr . "\n";
    $qh->execute( $target_date )
        or die "$scriptname: error running SQL query: " . $dbh->errstr . "\n";
}
$dbh->commit;

# end of party
$dbh->disconnect();
