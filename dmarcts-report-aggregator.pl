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
use DateTime;

my $scriptname = basename( $0 );

our ( $dbtype, $dbname, $dbuser, $dbpass, $dbhost, $dbport, $db_tx_support, $debug );

#$debug = 1;

sub show_usage {
    print <<EOF_USAGE
usage: $scriptname [-t <target_date>] [-b <days_before_today>]
    <target_date> in format 'YYYY-MM-DD'
EOF_USAGE
      ;
}

# Basic SQL query:
# Reports have mindate and maxdate fields, and may span more than one day.
# The messages for each report are charged to the mindate, so that we have a
# common criteria
my $basic_select_query = "SELECT COALESCE( SUM(rptrecord.rcount),0) AS total_rcount
  FROM report
  JOIN rptrecord ON report.serial = rptrecord.serial
  WHERE report.mindate >= ? AND report.mindate < ? \n";

# query fields
my @data_fields = qw(
  num_total
  num_rejected
  num_quarantined
  num_align_failed
  num_dkim_failed
  num_spf_failed
  num_spf_dkim_failed
  num_dkim_permerror
  num_spf_permerror
);

# SQL WHERE clauses for each of the aggregated fields
my %select_query_where = (
    'num_total'           => undef,
    'num_rejected'        => "disposition = 'reject'",
    'num_quarantined'     => "disposition = 'quarantine'",
    'num_align_failed'    => "spf_align <> 'pass' AND dkim_align <> 'pass'",
    'num_dkim_failed'     => "dkimresult = 'fail' AND spfresult <> 'fail'",
    'num_spf_failed'      => "dkimresult <> 'fail' AND spfresult = 'fail'",
    'num_spf_dkim_failed' => "dkimresult = 'fail' AND spfresult = 'fail'",
    'num_dkim_permerror'  => "dkimresult = 'permerror'",
    'num_spf_permerror'   => "spfresult = 'permerror'",
);

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

my $target_date_next;
if ( defined( $target_date ) ) {
    ( $target_date =~ m/^(\d{4})\-(\d{2})\-(\d{2})$/ )
      or die "$scriptname: <target_date> for -t must be in format YYYY-MM-DD\n";
    $target_date_next = DateTime->new(
        year   => $1,
        month  => $2,
        day    => $3,
        hour   => 0,
        minute => 0,
        second => 0,
    )->add( days => 1 )->strftime( '%Y-%m-%d' );
} else {
    ( defined( $days_before ) and ( $days_before =~ /^(\d+)$/ ) )
      or die "$scriptname: <n> for -b must be an integer\n";
    my $tmp_datetime = DateTime->now()->subtract( days => $1 );
    $target_date      = $tmp_datetime->strftime( '%Y-%m-%d' );
    $target_date_next = $tmp_datetime->add( days => 1 )->strftime( '%Y-%m-%d' );
}
printf( "Target date: %s\n",      $target_date )      if $debug;
printf( "Target date next: %s\n", $target_date_next ) if $debug;

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

# check that table 'metric' exists
my $query_handle = $dbh->prepare( "DESC metric" );
$query_handle->{RaiseError} = 0;
$query_handle->{PrintError} = 0;
if ( not $query_handle->execute() ) {
    die "$scriptname: table 'metric' not found, please run dmarcts-report-parser.pl at least once\n";
}
print "Table 'metric' exists\n" if $debug;
$query_handle->finish;

# build the query to update the metrics table and prepare the query for execution
# the VALUES are inserted as subqueries
my $aggregate_query = sprintf(
    "INSERT INTO metric(\n  date,\n  %s\n) VALUES (\n  ?,\n  %s\n)",
    join( ",\n  ", @data_fields ),    # for INTO
    join(
        ",\n  ",                      # for VALUES
        map {
                '('
              . $basic_select_query
              . (
                defined( $select_query_where{$_} )
                ? "  and " . $select_query_where{$_}
                : "  "
              )
              . ')'
        } @data_fields
    )
);
print "Aggregation query: $aggregate_query\n" if $debug;
$query_handle = $dbh->prepare( $aggregate_query );

# since each basic select query contains 2 '?' params (start date and end
# date) the full query needs to be passed that parameter pair, times the
# number of data fields, plus an additional one at the start for the record date
my $num_data_fields = scalar( @data_fields );
if ( not $query_handle->execute( $target_date, ( $target_date, $target_date_next ) x $num_data_fields, ) ) {
    die "$scriptname: error running SQL query: " . $dbh->errstr . "\n";
}
$dbh->commit;

# end of party
$dbh->disconnect();
