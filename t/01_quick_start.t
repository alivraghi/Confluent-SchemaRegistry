#!/bin/env perl

use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;
$Data::Dumper::Useqq = 1;

use Test::More qw( no_plan );

BEGIN { use_ok('Confluent::SchemaRegistry', qq/Using/); }

my $class = 'Confluent::SchemaRegistry';

my $sr;
$sr = new_ok($class => [ 'HOST','localhost', 'PORT','8081' ], qq/Custom host and port/);
$sr = undef;

$sr = $class->new('HOST','localhost1', 'PORT','8081');
ok(!defined($sr), qq/Invalid host/);
$sr = $class->new('HOST','localhost', 'PORT','8082');
ok(!defined($sr), qq/Invalid port/);

$sr = new_ok($class => [], qq/Default host and port/);

# Main AVRO schema
my $main_schema = {
	name => 'test_contacts',
	type => 'record',
	fields => [
				  {
					name => 'name',
					type => 'string'
				  },
				  {
					name => 'age',
					type => 'int'
				  }
				]
};
# Invalid AVRO schema
my $invalid_schema = {
};
# Backward compatible AVRO schema
my $compliant_schema = {
	name => 'test_contacts',
	type => 'record',
	fields => [
				  {
					name => 'name',
					type => 'string'
				  },
				  {
					name => 'age',
					type => 'int'
				  },
				  {
					name => 'gender',
					type => ['enum','null'],
					symbols => ['F', 'M'],
					default => undef
				  }
				]
};
# Non backward compatible AVRO schema
my $non_compliant_schema = {
	name => 'test_contacts',
	type => 'record',
	fields => [
				  {
					name => 'name',
					type => 'string'
				  },
				  {
					name => 'age',
					type => 'int'
				  },
				  {
					name => 'gender',
					type => ['enum','null'],
					symbols => ['F', 'M']
				  }
				]
};

my $subject = 'confluent-schema-registry-' . time;
my $type = 'value';

ok(!defined $sr->add_schema(), qq/Bad call to add_schema/);
ok(!defined $sr->add_schema(SUBJECT => $subject), qq/Bad call to add_schema/);
ok(!defined $sr->add_schema(SUBJECT => $subject, TYPE => ''), qq/Bad empty TYPE in call to add_schema/);
ok(!defined $sr->add_schema(SUBJECT => $subject, TYPE => 'foo'), qq/Bad TYPE in call to add_schema/);
ok(!defined $sr->add_schema(SUBJECT => $subject, TYPE => $type), qq/Bad call to add_schema/);
ok(!defined $sr->add_schema(SUBJECT => $subject, TYPE => $type), qq/Bad call to add_schema/);

my $error = $sr->add_schema(SUBJECT => $subject, TYPE => $type, SCHEMA => $invalid_schema);
isa_ok($error, 'HASH', qq/Invalid schema/);

my $new_id = $sr->add_schema(SUBJECT => $subject, TYPE => $type, SCHEMA => $main_schema);
like($new_id, qr/^\d+$/, qq/Good call to add_schema(SUBJECT=>'$subject', TYPE=>'$type', SCHEMA=>'...') returns $new_id/);

my $subjects = $sr->get_subjects();
isa_ok($subjects, 'ARRAY', qq/Subjects/);
print STDERR Dumper $subjects;

my $new_version = $sr->get_schema_versions(SUBJECT => $subject, TYPE => $type);
isa_ok($new_version, 'ARRAY', qq/Schema versions/);
ok(scalar(@$new_version)==1, qq/Only one version for current subject/);

my $loaded_schema = $sr->get_schema_by_id(SCHEMA_ID => $new_id);
is_deeply($loaded_schema, $main_schema, qq/Main vs loaded schema comparison/);



my $deleted = $sr->delete_subject(SUBJECT => $subject, TYPE => $type);
isa_ok($deleted, 'ARRAY', qq/Subject deletion/);


#
#print 'get_schema: ' . Dumper $sr->get_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', VERSION => 1);
#print 'get_schema (latest): ' . Dumper $sr->get_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value');
#print 'check_schema: ' . Dumper $sr->check_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema1);
#print 'check_schema: ' . Dumper $sr->check_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema2);
#print 'test_schema: ' . ($sr->test_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema1) ? 'is compatible' : 'is NOT compatible'), "\n";
#print 'test_schema: ' . ($sr->test_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema2) ? 'is compatible' : 'is NOT compatible'), "\n";

