#!/usr/bin/env perl
#
#use Config;
#use inc::Module::Install;
#
# 
#if ( $^O eq 'MSWin32' || $^O eq 'solaris' ||  $^O eq 'cygwin' ) {
#    print STDERR "OS unsupported: $^O\n";
#    exit( 255 );
#}
# 
#build_requires 'Test::More', 0.88;
#
#WriteMakefile(
#    NAME => 'Confluent::SchemaRegistry',
#    META_MERGE => {
#        resources => {
#	    	bugtracker => {
#	    		web => 'https://github.com/alivraghi/Confluent-SchemaRegistry/issues'
#	    	},
#            homapage => 'https://github.com/alivraghi/Confluent-SchemaRegistry',
#            repository => {
#            	type => 'git',
#            	url => 'git@github.com/Confluent-SchemaRegistry.git',
#            	web => 'https://github.com/alivraghi/Confluent-SchemaRegistry.git'
#            }
#        }
#    }
#);
#
#
##!/usr/bin/perl -w

use strict;
use warnings;
use 5.010;

if ( $^O eq 'MSWin32' || $^O eq 'solaris' ||  $^O eq 'cygwin' ) {
    print STDERR "OS unsupported: $^O\n";
    exit( 255 );
}

use ExtUtils::MakeMaker qw(
    prompt
);
use File::Spec::Functions qw(
    catdir
);

my %config = (
    NAME                => 'Confluent::SchemaRegistry',
    VERSION_FROM        => 'lib/Confluent/SchemaRegistry.pm',  # finds $VERSION
#    README_FROM         => 'lib/Confluent/SchemaRegistry.pm',  # finds $VERSION
    MIN_PERL_VERSION    => '5.010',
    PREREQ_PM           => {            # more to the list $Module::CoreList::version{5.010}
		'JSON::XS'		=> '3.02',
		'REST::Client'	=> '273',
		'Try::Tiny'		=> '0.22',
		'Avro::Schema'	=> 0,
		'Aspect'		=> '1.04'
    },
    TEST_REQUIRES       => {            # more to the list $Module::CoreList::version{5.010}
		'Math::BigInt'				=> '1.89',
		'Test::Exception'			=> '0.43'
    },
    META_MERGE          => {
#        no_index                => {
#            directory               => [
#                't',
#                'inc',
#                'xt',
#                'tools',
#            ]
#        },
#        provides                => {
#            'Confluent::SchemaRegistry'                 => {
#                file                    => 'lib/Confluent/SchemaRegistry.pm',
#                version                 => '0.02'
#            },
#            'Kafka::Connection'     => {
#                file                    => 'lib/Kafka/Connection.pm',
#                version                 => '1.07'
#            },
#            'Kafka::Consumer'       => {
#                file                    => 'lib/Kafka/Consumer.pm',
#                version                 => '1.07'
#            },
#            'Kafka::Exceptions'     => {
#                file                    => 'lib/Kafka/Exceptions.pm',
#                version                 => '1.07'
#            },
#            'Kafka::IO'             => {
#                file                    => 'lib/Kafka/IO.pm',
#                version                 => '1.07'
#            },
#            'Kafka::Int64'          => {
#                file                    => 'lib/Kafka/Int64.pm',
#                version                 => '1.07'
#            },
#            'Kafka::Internals'      => {
#                file                    => 'lib/Kafka/Internals.pm',
#                version                 => '1.07'
#            },
#            'Kafka::Message'        => {
#                file                    => 'lib/Kafka/Message.pm',
#                version                 => '1.07'
#            },
#            'Kafka::Producer'       => {
#                file                    => 'lib/Kafka/Producer.pm',
#                version                 => '1.07'
#            },
#            'Kafka::Protocol'       => {
#                file                    => 'lib/Kafka/Protocol.pm',
#                version                 => '1.07'
#            },
#        },
        'meta-spec'             => { version => 2 },
        resources               => {
            bugtracker              => {
               web                      => 'https://github.com/alivraghi/Confluent-SchemaRegistry/issues',
            },
            homepage                => 'https://github.com/alivraghi/Confluent-SchemaRegistry',
            repository              => {
                type                    => 'git',
                url                     => 'git@github.com/Confluent-SchemaRegistry.git',
                web                     => 'https://github.com/alivraghi/Confluent-SchemaRegistry.git',
            },
        },
    },
    ( $] >= 5.005 ?                             ## Add these new keywords supported since 5.005
        (
            ABSTRACT            => 'A simple client for interacting with Confluent Schema Registry.',
            AUTHOR              => 'Alvaro Livraghi',
        ) : ()
    ),
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => "Debian_CPANTS.txt Confluent-Schema-Registry-*.tar.gz" },
    LICENSE             => 'perl',
#    NO_MYMETA           => 1,
);

my @patterns = qw( t/*.t );
$config{test} = { TESTS => join ' ', map { glob } @patterns };

if ( $ENV{AUTHOR_TESTS} ) {
    @patterns = qw( xt/author/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

if ( $ENV{RELEASE_TESTS} ) {
    @patterns = qw( xt/release/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

mkdir 't/data' unless -d 't/data';
mkdir 'inc' unless -d 'inc';

ExtUtils::MakeMaker::WriteMakefile( %config );



