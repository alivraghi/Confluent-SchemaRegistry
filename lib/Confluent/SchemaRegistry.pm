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

Read full Schema Registry RESTful APIs here: L<https://docs.confluent.io/current/schema-registry/docs/api.html?_ga=2.234767710.1188695207.1526911788-1213051144.1524553242#>

=cut

use 5.010;
use strict;
use warnings;

use JSON::XS;
use REST::Client;
use HTTP::Status qw/:is/;
use Try::Tiny;
use Aspect;

use Avro::Schema;

# FIXME check http status in every call to RESTful APIS
# FIXME implement update_top_level_congig() and update_config() methods

our $VERSION = '0.01';


=head2 Constructor

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
	$self->{_ERROR} = undef;
	$self->{_RESPONSE} = undef;

	$self = bless($self, $class);

	#
	# BEGIN Using Aspect to simplify error handling
	#
	my $rest_client_calls = qr/^REST::Client::(GET|PUT|POST|DELETE)/;

	# Clear internal error and response before every REST::Client call
	before {
		$self->_clear_response();
		$self->_clear__error();
	} call $rest_client_calls;
	
	# Verify if REST calls are successfull 
	after {
		if (is_success($_->self->responseCode())) {
			$self->_set_response( $_->self->responseContent() );
			$_->return_value(1); # success
		} else {
			$self->_set_error( $_->self->responseContent() );
			$_->return_value(0); # failure
		}
		#print STDERR $_->self->responseCode() . "\n";
	} call $rest_client_calls;
	
	#
	# END Aspect
	#
	
	# Recupero la configurazione globale del registry per testare se le coordinate fanno
	# effettivamente riferimento ad un Confluent Schema Registry
	my $res = $self->get_top_level_config();
	return undef
		unless $res =~ m/^NONE|FULL|FORWARD|BACKWARD$/;
		
	return $self;
}


##############################################################################################
# PRIVATE METHODS
#
 
sub _client           { $_[0]->{_CLIENT}                                } # RESTful client
sub _clear__error     { $_[0]->_set_error(undef)                        } # clear internal error
sub _set_error        { $_[0]->{_ERROR} = $_[0]->_set_content($_[1])    } # set internal error
sub _get_error        { $_[0]->{_ERROR}                                 } # get internal error
sub _clear_response   { $_[0]->_set_response(undef)                     } # clear http response
sub _set_response     { $_[0]->{_RESPONSE} = $_[0]->_set_content($_[1]) } # save http response 
sub _get_response     { $_[0]->{_RESPONSE}                              } # return http response

sub _set_content { 
	my $self = shift;
	my $res = shift;
	return undef
		unless defined($res);
	return try {
		decode_json($res);
	} catch {
		$res;
	}
} 


##############################################################################################
# PUBLIC METHODS
#

=head2 METHODS

C<Confluent::SchemRegistry> exposes the following methods.

=cut


=head3 get_response_content()

Returns the body (content) of the last method call to Schema Registry.

=cut

sub get_response_content { $_[0]->_get_response() }
	

=head3 get_error()

Returns the error structure of the last method call to Schema Registry.

=cut

sub get_error { $_[0]->_get_error() }
	

=head3 add_schema( %params )

Registers a new schema version under a subject.

Returns the generated id for the new schema or C<undef>.

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
	return $self->_get_response()->{id}
		if $self->_client()->POST('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions', $schema);
	return undef;
}


# List all the registered subjects
#
# Returns the list of subjects (ARRAY) or C<undef>
sub get_subjects {
	my $self = shift;
	$self->_client()->GET('/subjects');
	return $self->_get_response();
}


# Delete a subject
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# SCHEMA....: the schema (HASH) to check for
#
# Returns the list of versions for the deleted subject or C<undef>
sub delete_subject {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	$self->_client()->DELETE('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE});
	return $self->_get_response()
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
	$self->_client()->GET('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions');
	return $self->_get_response();
}


# Fetch a schema by globally unique id
#
# SCHEMA_ID...: the globally unique id of the schema
#
# Returns the schema in Avro::Schema format or C<undef>
sub get_schema_by_id {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SCHEMA_ID})
				&& $params{SCHEMA_ID} =~ m/^\d+$/;
	if ( $self->_client()->GET('/schemas/ids/' . $params{SCHEMA_ID})) {
		if (exists $self->_get_response()->{schema}) {
			return try {
				Avro::Schema->parse($self->_get_response()->{schema});
			};
		}
	}
	return undef;
}


# Fetch a specific version of the schema registered under a subject
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# VERSION...: the schema version to fetch; if omitted the latest version is fetched
#
# Returns the schema in Avro::Schema format or C<undef>
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
	if ($self->_client()->GET('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions/' . $params{VERSION})) {
		if (exists $self->_get_response()->{schema}) {
			return try {
				Avro::Schema->parse($self->_get_response()->{schema});
			};
		}
	}
	return undef;
}


# Delete a specific version of the schema registered under a subject
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# VERSION...: the schema version to delete
#
# Returns the deleted version number (NUMBER) or C<undef>
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
	return $self->_get_response();
}


# Delete all versions of the schema registered under subject "Kafka-value"
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
#
# Returns the list of deleted versions or C<undef>
sub delete_all_schemas {
	my $self = shift;
	my %params = @_;
	return undef
		unless	defined($params{SUBJECT})
				&& defined($params{TYPE})
				&& $params{SUBJECT} =~ m/^.+$/
				&& $params{TYPE} =~ m/^key|value$/;
	$self->_client()->DELETE('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE});
	return $self->_get_response();
}


# Check whether the schema $SCHEMA has been registered under subject "${SUBJECT}-${TYPE}"
#
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# SCHEMA....: the schema (HASH) to check for
#
# If found, returns the schema info (HASH) otherwise C<undef>
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
	$self->_client()->POST('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE}, $schema);
	return $self->_get_response();
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
	$self->_client()->POST('/compatibility/subjects/' . $params{SUBJECT} . '-' . $params{TYPE} . '/versions/' . $params{VERSION}, encode_json($schema));
	return $self->_get_response()->{is_compatible}
		if exists($self->_get_response()->{is_compatible});
	return undef;
}


# Get top level config
#
# Return top-level compatibility level or C<undef>
sub get_top_level_config {
	my $self = shift;
	return $self->_get_response()->{compatibilityLevel}
		if $self->_client()->GET('/config');
	return undef;
}


# Update compatibility requirements globally
# $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#     --data '{"compatibility": "NONE"}' \
#     http://localhost:8081/config
#   {"compatibility":"NONE"}
sub update_top_level_config {
	# compatibility	STRING (NONE|FULL|FORWARD|BACKWARD)
	#
	# returns compatibility (STRING)
}


# Update compatibility requirements under the subject "Kafka-value"
# $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#     --data '{"compatibility": "BACKWARD"}' \
#     http://localhost:8081/config/Kafka-value
#   {"compatibility":"BACKWARD"}
sub update_config {
	# type			STRING ("key" || "value")
	# subject		STRING
	# compatibility	STRING (NONE|FULL|FORWARD|BACKWARD)
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
