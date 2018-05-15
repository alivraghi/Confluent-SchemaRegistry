package Confluent::SchemaRegistry;

use strict;
use warnings;

use JSON;
use REST::Client;
use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;
$Data::Dumper::Useqq = 1;


# protocol	STRING
# host		STRING
# port		NUMBER
sub new {
    my $this  = shift;
    my $class = ref($this) || $this;
	my %params = @_;
	my $self = {};
	
	# Verifica parametri di configurazione
	$self->{_PROTOCOL} = $params{PROTOCOL} || 'http';
	$self->{_HOST} = $params{HOST} || 'localhost';
	$self->{_PORT} = $params{PORT} || '8081';
	$self->{_BASE_URL} = $self->{_PROTOCOL} . '://' . $self->{_HOST} . ':' . $self->{_PORT};
	
	# Creazione client REST
	$self->{_CLIENT} = REST::Client->new( host => $self->{_BASE_URL} )
		or die "Unable to connect to $self->{_BASE_URL}: $!";
	
	# Recupero la configurazione globale del registry per testare se le coordinate fanno
	# effettivamente riferimento ad un Confluent Schema Registry 
	$self = bless($self, $class);
	my $tlc = $self->get_top_level_config();
	die "Invalid Schema Registry at $self->{_BASE_URL}: $!"
		unless $tlc =~ m/^BACKWARD|NONE$/;
	
	return $self;
}

sub _client { $_[0]->{_CLIENT} }

# # Register a new version of a schema under the subject "Kafka-key"
# $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#     --data '{"schema": "{\"type\": \"string\"}"}' \
#     http://localhost:8081/subjects/Kafka-key/versions
#   {"id":1}
# 
# # # Register a new version of a schema under the subject "Kafka-value"
# $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#     --data '{"schema": "{\"type\": \"string\"}"}' \
#      http://localhost:8081/subjects/Kafka-value/versions
#   {"id":1}
sub add_schema {
	# subject	STRING
	# type		STRING ("key" || "value")
	# schema	HASH
	# 
	# returns the version id (NUMBER)
}


# List all subjects
# $ curl -X GET http://localhost:8081/subjects
# 
# Returns the list of subjects (ARRAY) or the REST error (HASH)
sub get_subjects {
	my $self = shift;
	my $res = $self->_client()->GET('/subjects')->responseContent();
	return decode_json($res);
}


# List all schema versions registered under the subject "${SUBJECT}-${TYPE}"
# $ curl -X GET http://localhost:8081/subjects/${SUBJECT}-${TYPE}/versions
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


# Fetch a schema by globally unique id $SCHEMA_ID
# $ curl -X GET http://localhost:8081/schemas/ids/${SCHEMA_ID}
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
		$res->{schema} = decode_json($res->{schema});
	}
	return $res;
}


# Fetch version $VERSION of the schema registered under subject "${SUBJECT}-${TYPE}"
# $ curl -X GET http://localhost:8081/subjects/${SUBJECT}-${TYPE}/versions/${VERSION}
# 
# Fetch the most recently registered schema under subject "Kafka-value"
# $ curl -X GET http://localhost:8081/subjects/${SUBJECT}-${TYPE}/versions/latest
# 
# SUBJECT...: the name of the Kafka topic
# TYPE......: the type of schema ("key" or "value")
# VERSION...: the schema version to retrieve; if omitted latest version is returned
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
		$res->{schema} = decode_json($res->{schema});
	}
	return $res;
}


# # Delete version 3 of the schema registered under subject "Kafka-value"
# $ curl -X DELETE http://localhost:8081/subjects/Kafka-value/versions/3
#   3
sub delete_schema {
	# subject	STRING
	# type		STRING ("key" || "value")
	# version	NUMBER
	#
	# returns the number of deleted version (NUMBER)
}


# # Delete all versions of the schema registered under subject "Kafka-value"
# $ curl -X DELETE http://localhost:8081/subjects/Kafka-value
#   [1, 2, 3, 4, 5]
sub delete_all_schemas {
	# subject	STRING
	# type		STRING ("key" || "value")
	#
	# returns the list of deleted versions (NUMBER[])
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
	my $schema = {
		schema => encode_json($params{SCHEMA})
	};
	return decode_json($self->_client()->POST('/subjects/' . $params{SUBJECT} . '-' . $params{TYPE}, encode_json($schema))->responseContent());
}


# # Test compatibility of the schema $SCHEMA with the version $VERSION of the schema under subject "${SUBJECT}-${TYPE}"
# $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#     --data '{"schema": "{\"type\": \"string\"}"}' \
#     http://localhost:8081/compatibility/subjects/Kafka-value/versions/latest
#   {"is_compatible":true}
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


# # Get top level config
# $ curl -X GET http://localhost:8081/config
#   {"compatibilityLevel":"BACKWARD"}
sub get_top_level_config {
	my $self = shift;
	my $res = $self->_client()->GET('/config')->responseContent();
	if ($res) {
		$res = decode_json($res);
		return $res->{compatibilityLevel} || '';
	}
	return '';
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

1;