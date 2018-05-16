package Confluent::SchemaRegistry;

=head1 NAME

Confluent::SchemaRegistry - A simple client for interacting with B<Confluent Schema Registry>.

=head1 SYNOPSIS

 use Confluent::SchemaRegistry;

 my $sr = Confluent::SchemaRegistry->new( { host => 'https://my-schema-registry.org' });

=head1 DESCRIPTION

C<Confluent::SchemaRegistry> provides a simple way to interact with B<Confluent Schema Registry>
(L<https://docs.confluent.io/current/schema-registry/docs/index.html>) enabling writing into
B<Apache Kafka> (L<https://kafka.apache.org/>) according to I<Apache Avro> schema specification
(L<https://avro.apache.org/>).

=cut

=head1 METHODS

=cut

use 5.008;
use strict;
use warnings;

use JSON;
use REST::Client;

our $VERSION = '0.01';

=head2 Construction

=head3 new( [%config] )

Construct a new C<Confluent::SchemaRegistry>. Takes an optional hash that provides
configuration flags for the L<REST::Client> internal object.

The config flags, according to C<REST::Client::new> specs, are:

=over 4

=item host

The host at which I<Schema Registry> is listening.

The default is L<http://localhost:8081>

=item timeout

A timeout in seconds for requests made with the client.  After the timeout the
client will return a 500.

The default is 5 minutes.

=item cert

The path to a X509 certificate file to be used for client authentication.

The default is to not use a certificate/key pair.

=item key

The path to a X509 key file to be used for client authentication.

The default is to not use a certificate/key pair.

=item ca

The path to a certificate authority file to be used to verify host
certificates.

The default is to not use a certificates authority.

=item pkcs12

The path to a PKCS12 certificate to be used for client authentication.

=item pkcs12password

The password for the PKCS12 certificate specified with 'pkcs12'.

=item follow

Boolean that determins whether REST::Client attempts to automatically follow
redirects/authentication.

The default is false.

=item useragent

An L<LWP::UserAgent> object, ready to make http requests.

REST::Client will provide a default for you if you do not set this.

=back

=cut
sub new {
    my $this  = shift;
    my $class = ref($this) || $this;
	my %config = @_;
	my $self = {};

	# Creazione client REST
	$config{host} = 'http://localhost:8081' unless defined $config{host};
	$self->{_CLIENT} = REST::Client->new( %config );
	$self->{_CLIENT}->addHeader('Content-Type', 'application/vnd.schemaregistry.v1+json');

	# Recupero la configurazione globale del registry per testare se le coordinate fanno
	# effettivamente riferimento ad un Confluent Schema Registry
	$self = bless($self, $class);
	my $tlc = $self->get_top_level_config();
	if ($tlc !~ m/^BACKWARD|NONE$/) {
		return undef;
	} else {
		return $self;
	}
}


# Private method that returns REST client
sub _client { $_[0]->{_CLIENT} }


=head3 add_schema( %params )

Registers a new schema version under a subject.

Returns the generated id for the new schema or a RESTful error.

Params keys are:

=over 4

=item SUBJECT ($scalar)

the name of the Kafka topic

=item TYPE ($scalar)

the type of schema ("key" or "value")

=item SCHEMA ($hashref)

the schema to add

=back

=cut
sub add_schema {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	return undef
		unless	defined($params{SCHEMA})
				&& ref($params{SCHEMA}) eq 'HASH';
	my $schema = encode_json({
		schema => encode_json($params{SCHEMA})
	});
	my $res = decode_json($self->_client()->POST('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions', $schema)->responseContent());
	return $res->{id} if exists $res->{id};
	return $res;
}


# List all the registered subjects
#
# Returns the list of subjects (ARRAY) or the REST error (HASH)
sub get_subjects {
	my $self = shift;
	my $res = $self->_client()->GET('/subjects')->responseContent();
	return decode_json($res);
}


# Delete a subject
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# SCHEMA....: the schema (HASH) to check for
#
# Returns the list of versions for the deleted subject or the REST error
sub delete_subject {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	my $res = decode_json($self->_client()->DELETE('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE})->responseContent());
	return $res;

}


# List the schema versions registered under a subject
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
#
# Returns the list of schema versions (ARRAY)
sub get_schema_versions {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	return decode_json($self->_client()->GET('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions')->responseContent());
}


# Fetch a schema by globally unique id
#
# SCHEMA_ID...: the globally unique id of the schema
#
# Returns schema (HASH) or the REST error
sub get_schema_by_id {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SCHEMA_ID})
				&& $params{SCHEMA_ID} =~ m/^\d+$/;
	my $res = decode_json($self->_client()->GET('/schemas/ids/' . $params{SCHEMA_ID})->responseContent());
	if (exists $res->{schema}) {
		return decode_json($res->{schema});
	}
	return $res;
}


# Fetch a specific version of the schema registered under a subject
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# VERSION...: the schema version to fetch; if omitted the latest version is fetched
#
# Returns schema (HASH) or the REST error
sub get_schema {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	return undef
		if	defined($params{VERSION})
			&& $params{VERSION} !~ m/^\d+$/;
	$params{VERSION} = 'latest' unless defined($params{VERSION});
	my $res = decode_json($self->_client()->GET('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions/' . $params{VERSION})->responseContent());
	if (exists $res->{schema}) {
		return decode_json($res->{schema});
	}
	return $res;
}


# Delete a specific version of the schema registered under a subject
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# VERSION...: the schema version to delete
#
# Returns the deleted version number (NUMBER)
sub delete_schema {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	return undef
		unless	defined($params{VERSION})
				&& $params{VERSION} =~ m/^\d+$/;
	$self->_client()->DELETE('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions/' . $params{VERSION});
	my $res = $self->_client()->responseContent();
	if ( $self->_client()->responseCode() >= 200 && $self->_client()->responseCode() < 300 ) {
		return $res;
	} else {
		return decode_json($res);
	}
}


# Delete all versions of the schema registered under subject "Kafka-value"
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
#
# Returns the list of deleted versions
sub delete_all_schemas {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	$self->_client()->DELETE('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE});
	my $res = decode_json($self->_client()->responseContent());
}


# Check whether the schema $SCHEMA has been registered under subject "${SUBJECT}-${TYPE}"
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# SCHEMA....: the schema (HASH) to check for
#
# If found, returns the schema info (HASH) otherwise a REST error
sub check_schema {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	return undef
		unless	defined($params{SCHEMA})
				&& ref($params{SCHEMA}) eq 'HASH';
	my $schema = encode_json({
		schema => encode_json($params{SCHEMA})
	});
	return decode_json($self->_client()->POST('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE}, $schema)->responseContent());
}


# Test compatibility of the schema $SCHEMA with the version $VERSION of the schema under subject "${SUBJECT}-${TYPE}"
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# VERSION...: the schema version to test; if omitted latest version is used
# SCHEMA....: the schema (HASH) to check for
#
# returns TRUE if the providied schema is compatible with the latest one (BOOLEAN)
sub test_schema {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	return undef
		if	defined($params{VERSION})
			&& $params{VERSION} !~ m/^\d+$/;
	$params{VERSION} = 'latest' unless defined($params{VERSION});
	return undef
		unless	defined($params{SCHEMA})
				&& ref($params{SCHEMA}) eq 'HASH';
	my $schema = {
		schema => encode_json($params{SCHEMA})
	};
	my $res = decode_json($self->_client()->POST('/compatibility/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions/' . $params{VERSION}, encode_json($schema))->responseContent());
	if (exists($res->{is_compatible})) {
		return $res->{is_compatible};
	}
	return $res;
}


# Get top level config
#
# Return top-level compatibility level
sub get_top_level_config {
	my $self = shift;
	my $res = $self->_client()->GET('/config')->responseContent();
	return '' unless $res;
	local $@;
	eval {
		$res = decode_json($res);
	};
	return '' if $@;
	return $res->{compatibilityLevel} || '';
}


# # Update compatibility requirements globally
# $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#     --data '{"compatibility": "NONE"}' \
#     http://localhost:8081/config
#   {"compatibility":"NONE"}
sub update_top_level_config {
	# compatibility	STRING ('NONE' || 'BACKWARD')
	#
	# returns compatibility (STRING)
}


# # Update compatibility requirements under the subject "Kafka-value"
# $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#     --data '{"compatibility": "BACKWARD"}' \
#     http://localhost:8081/config/Kafka-value
#   {"compatibility":"BACKWARD"}
sub update_config {
	# type			STRING ("key" || "value")
	# subject		STRING
	# compatibility	STRING ('NONE' || 'BACKWARD')
	#
	# returns compatibility (STRING)
}

=head1 TODO

...

=head1 AUTHOR

Alvaro Livraghi, E<lt>alvarol@cpan.orgE<gt>

=head1 COPYRIGHT

Copyright 2008 - 2010 by Alvaro Livraghi

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

1;