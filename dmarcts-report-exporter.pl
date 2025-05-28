#!/usr/bin/perl

################################################################################
# dmarcts-report-exporter - A Perl based web application to export daily
# metrics in Prometheus format.  It returns yesterday's metric record if
# available, or no metrics otherwise
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
use Net::Prometheus;
use Plack::Builder;
use Plack::Runner;
use Data::Dumper;

my $scriptname = basename( $0 );

our ( $dbtype, $dbname, $dbuser, $dbpass, $dbhost, $dbport, $db_tx_support, $debug );

sub show_usage {
    print <<EOF_USAGE
usage: $scriptname -b <days_before_today>
    - Server metrics for today or N days before today in Prometheus format
EOF_USAGE
;
}

# locate conf file or die
my $conf_file = 'dmarcts-report.conf';
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

# parse cli options
# -b <n>: show data for today -n days (n=1 -> yesterday)
our ( $opt_b, $opt_h );
getopts( "b:h" );
defined( $opt_h ) and do {
  show_usage();
  exit;
};

my $days_before = $opt_b || undef;
defined( $opt_b ) or do {
  show_usage();
  exit;
};

# Load Database configuration
$db_tx_support  = 1;
our %dbx;
my $dbx_file = File::Basename::dirname($0) . "/dbx_$dbtype.pl";
my $dbx_return = do $dbx_file;
die "$scriptname: couldn't load DB definition for type $dbtype: $@" if $@;
die "$scriptname: couldn't load DB definition for type $dbtype: $!" unless defined $dbx_return;

# Database connection function, with reconnection policy
my $persistent_dbh;
my $first_conn = 1;
my $persistent_db_conn_retry = 5;	# seconds
sub get_dbh {
    while ( not defined( $persistent_dbh ) or not db_connection_alive( $persistent_dbh ) ) {
        if ( not $first_conn ) {
            warn "Lost database connection, reconnecting...\n";
        }
        $first_conn = 0;

        $persistent_dbh = DBI->connect(
            "DBI:$dbtype:database=$dbname;host=$dbhost;port=$dbport",
            $dbuser, $dbpass
        );
        last if defined( $persistent_dbh );
        sleep $persistent_db_conn_retry;
        warn "Could not connect to database, retrying...\n";
    }

    if ($db_tx_support) {
        $persistent_dbh->{ AutoCommit } = 0;
    }
    print "Database connection established\n" if $debug;
    return $persistent_dbh;
}

sub db_connection_alive {
    my $dbh = shift;
    return 0 unless defined $dbh;
    return eval { $dbh->ping } ? 1 : 0;
}

# helper functions to keep DB state for Prometheus
# we keep a cached value for the metrics
# we'll configure the Prometheus collectors to call this function for each metric
my $cache_expiry_time = 10;	# seconds
my $cached_metrics = { };
sub get_metric {
  my $metric = shift;

  # check if the metrics have been loaded from the DB and are not stale
  # if they are, load them and store in the cache with the timestamp
  if ( ( $cached_metrics->{ 'last_time' } || 0 ) < ( time - $cache_expiry_time ) ) {
    my $target_date = DateTime->now->subtract( days => $days_before )->strftime('%Y-%m-%d');
    my $metrics_query = sprintf( "SELECT * FROM metric WHERE DATE = '%s'", $target_date );
    my $dbh = get_dbh;
    my $all_metrics = $dbh->selectrow_hashref( $metrics_query );
    $cached_metrics = {
      'last_time'	=> time,
      'values'		=> $all_metrics,
    };
  }

  # now we are sure metrics are up to date
  return (
    defined( $cached_metrics->{'values'}{ $metric } ) ?
    $cached_metrics->{'values'}{ $metric } :
    undef );
}

# Configure Prometheus collectors
my %metrics = (
  'num_total'		=> 'Total messages',
  'num_rejected'	=> 'Rejected messages',
  'num_quarantined'	=> 'Quarantined messages',
  'num_align_failed'	=> 'Messages that pass SPF and DKIM but with strange From: address (probable misconfiguration)',
  'num_dkim_failed'	=> 'Messages that pass SPF but not DKIM (probable DKIM misconfiguration)',
  'num_spf_failed'	=> 'Messages that pass DKIM but not SPF (probable SPF record misconfiguration, missing IPs)',
  'num_spf_dkim_failed'	=> 'Messages that do not pass SPF nor DKIM (spam)',
  'num_dkim_permerror'	=> 'Messages with a permanent DKIM error',
  'num_spf_permerror'	=> 'Messages with a permanent SPF error',
);
my $prom = Net::Prometheus->new(
  disable_process_collector => 1,
  disable_perl_collector => 1,
);
foreach my $metric ( keys %metrics ) {
  # create each metric as a gauge and configure with a function to call the
  # metric helper function above when needed
  $prom->new_gauge( 'name' => $metric, 'help' => $metrics{ $metric } )->set_function( sub { get_metric( $metric ) } );
}

# Configure and create a PSGI Prometheus exporter app
my $app = builder {
   mount "/metrics" => $prom->psgi_app;
};

# Run the PSGI as a standalone web app
my $runner = Plack::Runner->new;
$runner->parse_options(
  '--port' => 9100,
);
$runner->run( $app );

# end of party
$persistent_dbh->disconnect();
