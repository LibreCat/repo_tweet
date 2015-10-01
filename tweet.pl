#!/usr/bin/env perl

use Catmandu::Sane;
use Catmandu qw(:load);
use POSIX qw(strftime);
use Net::Twitter::Lite::WithAPIv1_1;
use Data::Dumper;
use Getopt::Long;

sub _timestamp {
    strftime("%Y-%m-%dT%H:%M:%SZ", gmtime(time));
}

my $oai;
my $csv;
my $fix_file;
my $file;
my $url;
my $set;
my $metadataPrefix = 'oai_dc';
my $from;
my $until;
my $limit = Catmandu->config->{tweet_limit};
my $v;
my $x;
my $debug;
my $help;

GetOptions(
    'fix=s' => \$fix_file,
    'oai' => \$oai,
    'csv' => \$csv,
    'file=s' => \$file,
    'url=s' => \$url,
    'set=s' => \$set,
    'metadataPrefix=s' => \$metadataPrefix,
    'from=s' => \$from,
    'until=s' => \$until,
    'limit=i' => \$limit,
    'verbose|v' => \$v,
    'execute|x' => \$x,
    'help|h|?' => \$help,
    'debug' => \$debug,
);

if ($help || (!$oai && !$csv)) {
    print <<EOF;
Usage:    $0 --help|oai|csv [options]
Examples: $0 --csv --file records.csv
          $0 --oai --set economics --from 2014-01-01
Options:  oai:     tweet records harvested from a OAI endpoint (defined in
                   catmandu.yml)
          csv:     tweet records from a csv file (format '_id,title',
                   '_id,url,title' or '_id,tweet' defined in catmandu.yml)
          file:    path to csv file (default is stdin)
          fix:     fix file to operate on the record (optional)
          url:     oai baseURL to use (optional)
          metadataPrefix: oai metadataPrefix to use (optional)
          set:     oai set to use (optional)
          from:    lower bound for oai harvest (optional)
          until:   upper bound for oai harvest (optional)
          limit:   maximum number of tweets to send in one run (default
                   is 15)
          verbose: log tweets to stdout
          v:       see verbose
          help:    this help message
          h:       see help
          ?:       see help
          execute: actually send the tweets
          x:       see execute
          debug:   debugging messages
See the catmandu.yml file for more configuration options.
EOF
    exit 0;
}

my $twitter_client; 
if ($x) {
    my $twitter_credentials = Catmandu->config->{twitter_credentials};
    $twitter_client = Net::Twitter::Lite::WithAPIv1_1->new(
        %$twitter_credentials,
        ssl => 1,
    );
}

my $in_name = $csv ? 'csv' : 'oai';
my %in_opts;
if ($csv) {
    $in_opts{file} = $file if $file;
} else {
    $in_opts{url} = $url if $url;
    $in_opts{metadataPrefix} = $metadataPrefix if $metadataPrefix;
    $in_opts{set} = $set if $set;
    $in_opts{from} = $from if $from;
    $in_opts{until} = $until if $until;
}

print STDERR "importer $in_name: " . Dumper(\%in_opts) if $debug;

my $in = Catmandu->importer($in_name, %in_opts);

my $fixer;

if (defined $fix_file) {
  $fixer = Catmandu->fixer($fix_file);
}
elsif ($csv) {
  $fixer = Catmandu->fixer('csv');
}
elsif ($oai) {
  $fixer = Catmandu->fixer($metadataPrefix);
}
else {
  $fixer = Catmandu->fixer('null()');
}

print STDERR "fixer: $fixer\n" if $debug;

my $log = Catmandu->store('tweet_log')->bag('tweets');

my $log_file = Catmandu->exporter('tweet_log');

my $url_tmpl = Catmandu->config->{url_template};

$fixer->fix($in)->reject(sub { $log->get($_[0]->{_id}) })
    ->take($limit)
    ->each(sub {
        my $rec = $_[0];

        print STDERR "Got: " . Dumper($rec) if $debug;

        die "need an _id" unless $rec->{_id};

        my $id = $rec->{_id};

        my $tweet = $rec->{tweet} || do {
            die "need a url and title" unless ($rec->{url} && $rec->{title});

            my $url = $rec->{url};

            my $title = $rec->{title};

            my $msg;
            if (length($url) + length($title) + 1 > 140) {
                $msg = substr($title, 0, 140 - length($url) - 4) . "... $url";
            } else {
                $msg = "$title $url";
            }
            $msg;
        };

        my $log_data = {
            _id => $id,
            tweet => $tweet,
            timestamp => _timestamp,
        };

        if ($x) {
            print STDERR "Sending tweet: $tweet\n" if $debug;
            try {
                $twitter_client->update($tweet);
                $log->add($log_data);
            } catch {
                warn "[ERROR] $_";
            }
        }

        if ($v) {
           $log_file->add($log_data); 
        }

    });

$log_file->commit if $v; 

