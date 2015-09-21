#!/usr/bin/env perl

use Catmandu::Sane;
use Catmandu qw(:load);
use POSIX qw(strftime);
use Net::Twitter::Lite::WithAPIv1_1;
use Getopt::Long;

sub _timestamp {
    strftime("%Y-%m-%dT%H:%M:%SZ", gmtime(time));
}

my $oai;
my $csv;
my $file;
my $set;
my $from;
my $until;
my $limit = Catmandu->config->{tweet_limit};
my $v;
my $x;
my $help;

GetOptions(
    'oai' => \$oai,
    'csv' => \$csv,
    'file=s' => \$file,
    'set=s' => \$set,
    'from=s' => \$from,
    'until=s' => \$until,
    'limit=i' => \$limit,
    'verbose|v' => \$v,
    'execute|x' => \$x,
    'help|h|?' => \$help,
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
    $in_opts{set} = $set if $set;
    $in_opts{from} = $from if $from;
    $in_opts{until} = $until if $until;
}

my $in = Catmandu->importer($in_name, %in_opts);

my $log = Catmandu->store('tweet_log')->bag('tweets');

my $log_file = Catmandu->exporter('tweet_log');

my $url_tmpl = Catmandu->config->{url_template};

$in->reject(sub { $log->get($_[0]->{_id}) })
    ->take($limit)
    ->each(sub {
        my $rec = $_[0];

        my $id = $rec->{_id};

        my $tweet = $rec->{tweet} || do {
            my $url = $rec->{url} || sprintf($url_tmpl, $id);

            my $title = $rec->{title};
            $title = $title->[0] if ref $title;

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

