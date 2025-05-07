#!/usr/bin/perl

################################################################################
# dmarcts-report-aggregator - A Perl based tool to periodically aggregate
# daily DMARC reports in order to extract metrics and set up alerts if
# needed
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

my $scriptname = basename( $0 );

our ( $dbtype, $dbname, $dbuser, $dbpass, $dbhost, $dbport, $db_tx_support, $debug );

sub show_usage {
    print <<EOF_USAGE
usage: $scriptname [-t <target_date>] [-b <days_before_today>]
    <target_date> in format 'YYYY-MM-DD'
EOF_USAGE
;
}

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
    ( $target_date =~ m/^\d{4}\-\d{2}\-\d{2}$/ ) or
        die "$scriptname: <target_date> for -t must be in format YYYY-MM-DD\n";
} else {
    ( defined( $days_before ) and ( $days_before =~ /^\d+$/ ) ) or
        die "$scriptname: <n> for -b must be an integer\n";
    $target_date = strftime( '%Y-%m-%d', localtime() - $days_before * 24 * 3600 );
}
printf( "Target date: %s\n", $target_date ) if $debug;

my $conf_file = 'dmarcts-report-aggregator.conf';

# locate conf file or die
if ( -e $conf_file ) {
  #$conf_file = "./$conf_file";
} elsif( -e  (File::Basename::dirname($0) . "/$conf_file" ) ) {
	$conf_file = ( File::Basename::dirname($0) . "/$conf_file" );
} else {
	show_usage();
	die "$scriptname: Could not read config file '$conf_file' from current working directory or path (" . File::Basename::dirname($0) . ')'
}

# load conf file with error handling
if ( substr($conf_file, 0, 1) ne '/'  and substr($conf_file, 0, 1) ne '.') {
  $conf_file = "./$conf_file";
}
my $conf_return = do $conf_file;
die "$scriptname: couldn't parse $conf_file: $@" if $@;
die "$scriptname: couldn't do $conf_file: $!"    unless defined $conf_return;
print "Configuration loaded\n" if $debug;

# Setup connection to database server.
$db_tx_support  = 1;
our %dbx;
my $dbx_file = File::Basename::dirname($0) . "/dbx_$dbtype.pl";
my $dbx_return = do $dbx_file;
die "$scriptname: couldn't load DB definition for type $dbtype: $@" if $@;
die "$scriptname: couldn't load DB definition for type $dbtype: $!" unless defined $dbx_return;

my $dbh = DBI->connect("DBI:$dbtype:database=$dbname;host=$dbhost;port=$dbport",
        $dbuser, $dbpass)
or die "$scriptname: Cannot connect to database\n";
if ($db_tx_support) {
        $dbh->{AutoCommit} = 0;
}
print "Database connection established\n" if $debug;

# check that table 'metric' exists
my $query_handle = $dbh->prepare("DESC metric");
$query_handle->{RaiseError} = 0;
$query_handle->{PrintError} = 0;
if ( not $query_handle->execute() ) {
    die "$scriptname: table 'metric' not found, please run dmarcts-report-parser.pl at least once\n";
};
print "Table 'metric' exists\n" if $debug;

# end of party
$dbh->disconnect();
printf( "Target date: %s\n", $target_date ) if $debug;
